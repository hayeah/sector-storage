package sectorstorage

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/sector-storage/ffiwrapper"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/stores"
	"github.com/filecoin-project/sector-storage/storiface"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var log = logging.Logger("advmgr")

var ErrNoWorkers = errors.New("no suitable workers found")

type URLs []string

type Worker interface {
	ffiwrapper.StorageSealer

	TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error)

	// Returns paths accessible to the worker
	Paths(context.Context) ([]stores.StoragePath, error)

	Info(context.Context) (storiface.WorkerInfo, error)

	Close() error

	ID() string
}

type SectorManager interface {
	SectorSize() abi.SectorSize

	ReadPieceFromSealedSector(context.Context, abi.SectorID, ffiwrapper.UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) (io.ReadCloser, error)

	ffiwrapper.StorageSealer
	storage.Prover
}

type WorkerID uint64

type Manager struct {
	scfg *ffiwrapper.Config

	ls         stores.LocalStorage
	storage    *stores.Remote
	localStore *stores.Local
	remoteHnd  *stores.FetchHandler
	index      stores.SectorIndex

	storage.Prover

	workersLk  sync.Mutex
	nextWorker WorkerID
	workers    map[WorkerID]*workerHandle

	newWorkers chan *workerHandle
	schedule   chan *workerRequest
	workerFree chan WorkerID
	closing    chan struct{}

	sectorToWorkers *sectorToWorkers

	schedQueue *list.List // List[*workerRequest]
}

type SealerConfig struct {
	// Local worker config
	AllowPreCommit1 bool
	AllowPreCommit2 bool
	AllowCommit     bool
}

type StorageAuth http.Header

func New(ctx context.Context, ls stores.LocalStorage, si stores.SectorIndex, cfg *ffiwrapper.Config, sc SealerConfig, urls URLs, sa StorageAuth) (*Manager, error) {
	lstor, err := stores.NewLocal(ctx, ls, si, urls)
	if err != nil {
		return nil, err
	}

	prover, err := ffiwrapper.New(&readonlyProvider{stor: lstor}, cfg)
	if err != nil {
		return nil, xerrors.Errorf("creating prover instance: %w", err)
	}

	stor := stores.NewRemote(lstor, si, http.Header(sa))

	wd, err := homedir.Expand("~/.lotusstorage")
	if err != nil {
		return nil, err
	}

	sectorToWorkers, err := openSectorToWorkers(filepath.Join(wd, "sectorToWorkers.json"))
	if err != nil {
		return nil, err
	}

	m := &Manager{
		scfg: cfg,

		ls:         ls,
		storage:    stor,
		localStore: lstor,
		remoteHnd:  &stores.FetchHandler{Local: lstor},
		index:      si,

		nextWorker: 0,
		workers:    map[WorkerID]*workerHandle{},

		newWorkers: make(chan *workerHandle),
		schedule:   make(chan *workerRequest),
		workerFree: make(chan WorkerID),
		closing:    make(chan struct{}),

		schedQueue: list.New(),

		sectorToWorkers: sectorToWorkers,

		Prover: prover,
	}

	go m.runSched()

	// localTasks := []sealtasks.TaskType{
	// sealtasks.TTAddPiece,
	// sealtasks.TTCommit1,
	// sealtasks.TTFinalize,
	// }
	// if sc.AllowPreCommit1 {
	// 	localTasks = append(localTasks, sealtasks.TTPreCommit1)
	// }
	// if sc.AllowPreCommit2 {
	// 	localTasks = append(localTasks, sealtasks.TTPreCommit2)
	// }
	// if sc.AllowCommit {
	// 	localTasks = append(localTasks, sealtasks.TTCommit2)
	// }

	// err = m.AddWorker(ctx, NewLocalWorker(WorkerConfig{
	// 	SealProof: cfg.SealProofType,
	// 	TaskTypes: localTasks,
	// }, stor, lstor, si))
	// if err != nil {
	// 	return nil, xerrors.Errorf("adding local worker: %w", err)
	// }

	return m, nil
}

func (m *Manager) AddLocalStorage(ctx context.Context, path string) error {
	path, err := homedir.Expand(path)
	if err != nil {
		return xerrors.Errorf("expanding local path: %w", err)
	}

	if err := m.localStore.OpenPath(ctx, path); err != nil {
		return xerrors.Errorf("opening local path: %w", err)
	}

	if err := m.ls.SetStorage(func(sc *stores.StorageConfig) {
		sc.StoragePaths = append(sc.StoragePaths, stores.LocalPath{Path: path})
	}); err != nil {
		return xerrors.Errorf("get storage config: %w", err)
	}
	return nil
}

func (m *Manager) AddWorker(ctx context.Context, w Worker) error {
	info, err := w.Info(ctx)
	if err != nil {
		return xerrors.Errorf("getting worker info: %w", err)
	}

	log.Info("worker added:", w.ID())

	m.newWorkers <- &workerHandle{
		w:    w,
		info: info,
	}
	return nil
}

func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.remoteHnd.ServeHTTP(w, r)
}

func (m *Manager) SectorSize() abi.SectorSize {
	sz, _ := m.scfg.SealProofType.SectorSize()
	return sz
}

func (m *Manager) ReadPieceFromSealedSector(context.Context, abi.SectorID, ffiwrapper.UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) (io.ReadCloser, error) {
	panic("implement me")
}

func (m *Manager) getWorkerForSector(ctx context.Context, sid abi.SectorID) (Worker, func(), error) {
	// map the sector to the worker uuid, then to the worker's scheduler ID
	wid, ok := m.sectorToWorkers.Get(sid)
	if !ok {
		return nil, nil, xerrors.Errorf("cannot find worker %s for sector %s", wid, sid)
	}

	for workerID, w := range m.workers {
		if w.info.ID == wid {
			worker, done, err := m.getWorker(ctx, sealtasks.TTPreCommit1, []WorkerID{workerID})
			if err != nil {
				return nil, nil, xerrors.Errorf("scheduling worker: %w", err)
			}

			return worker, done, nil
		}
	}

	return nil, nil, xerrors.Errorf("cannot find worker %s for sector %s", wid, sid)
}

func (m *Manager) getWorkersByPaths(task sealtasks.TaskType, inPaths []stores.StorageInfo) ([]WorkerID, map[WorkerID]stores.StorageInfo) {
	m.workersLk.Lock()
	defer m.workersLk.Unlock()

	var workers []WorkerID
	paths := map[WorkerID]stores.StorageInfo{}

	for i, worker := range m.workers {
		tt, err := worker.w.TaskTypes(context.TODO())
		if err != nil {
			log.Errorf("error getting supported worker task types: %+v", err)
			continue
		}
		if _, ok := tt[task]; !ok {
			log.Debugf("dropping worker %d; task %s not supported (supports %v)", i, task, tt)
			continue
		}

		phs, err := worker.w.Paths(context.TODO())
		if err != nil {
			log.Errorf("error getting worker paths: %+v", err)
			continue
		}

		// check if the worker has access to the path we selected
		var st *stores.StorageInfo
		for _, p := range phs {
			for _, meta := range inPaths {
				if p.ID == meta.ID {
					if st != nil && st.Weight > p.Weight {
						continue
					}

					p := meta // copy
					st = &p
				}
			}
		}
		if st == nil {
			log.Debugf("skipping worker %d; doesn't have any of %v", i, inPaths)
			log.Debugf("skipping worker %d; only has %v", i, phs)
			continue
		}

		paths[i] = *st
		workers = append(workers, i)
	}

	return workers, paths
}

func (m *Manager) getWorker(ctx context.Context, taskType sealtasks.TaskType, accept []WorkerID) (Worker, func(), error) {
	ret := make(chan workerResponse)

	select {
	case m.schedule <- &workerRequest{
		taskType: taskType,
		accept:   accept,

		cancel: ctx.Done(),
		ret:    ret,
	}:
	case <-m.closing:
		return nil, nil, xerrors.New("closing")
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.worker, resp.done, resp.err
	case <-m.closing:
		return nil, nil, xerrors.New("closing")
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (m *Manager) NewSector(ctx context.Context, sector abi.SectorID) error {
	log.Warnf("stub NewSector")
	return nil
}

func (m *Manager) AddDummyPiece(ctx context.Context, sector abi.SectorID, existingPieces []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize) (abi.PieceInfo, error) {
	return abi.PieceInfo{}, xerrors.Errorf("not implemented")
}

func (m *Manager) AddPiece(ctx context.Context, sector abi.SectorID, existingPieces []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader) (abi.PieceInfo, error) {
	// TODO: consider multiple paths vs workers when initially allocating

	var best []stores.StorageInfo
	var err error
	if len(existingPieces) == 0 { // new
		best, err = m.index.StorageBestAlloc(ctx, stores.FTUnsealed, true)
	} else { // append to existing
		best, err = m.index.StorageFindSector(ctx, sector, stores.FTUnsealed, false)
	}
	if err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("finding sector path: %w", err)
	}

	log.Debugf("find workers for %v", best)
	candidateWorkers, _ := m.getWorkersByPaths(sealtasks.TTAddPiece, best)

	if len(candidateWorkers) == 0 {
		return abi.PieceInfo{}, ErrNoWorkers
	}

	// shuffle the candidate workers to load balance
	rand.Shuffle(len(candidateWorkers), func(i, j int) {
		candidateWorkers[i], candidateWorkers[j] = candidateWorkers[j], candidateWorkers[i]
	})

	worker, done, err := m.getWorker(ctx, sealtasks.TTAddPiece, candidateWorkers)
	if err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("scheduling worker: %w", err)
	}
	defer done()

	pinfo, err := worker.AddDummyPiece(ctx, sector, existingPieces, sz)

	err = m.sectorToWorkers.Put(sector, worker.ID())
	if err != nil {
		return abi.PieceInfo{}, err
	}

	return pinfo, err
}

func (m *Manager) SealPreCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, pieces []abi.PieceInfo) (out storage.PreCommit1Out, err error) {
	worker, done, err := m.getWorkerForSector(ctx, sector)
	if err != nil {
		return nil, xerrors.Errorf("scheduling worker: %w", err)
	}
	defer done()

	// TODO: select(candidateWorkers, ...)
	// TODO: remove the sectorbuilder abstraction, pass path directly
	return worker.SealPreCommit1(ctx, sector, ticket, pieces)
}

func (m *Manager) SealPreCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.PreCommit1Out) (cids storage.SectorCids, err error) {
	worker, done, err := m.getWorkerForSector(ctx, sector)
	if err != nil {
		return storage.SectorCids{}, xerrors.Errorf("scheduling worker: %w", err)
	}
	defer done()

	// TODO: select(candidateWorkers, ...)
	// TODO: remove the sectorbuilder abstraction, pass path directly
	return worker.SealPreCommit2(ctx, sector, phase1Out)
}

func (m *Manager) SealCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (output storage.Commit1Out, err error) {
	worker, done, err := m.getWorkerForSector(ctx, sector)
	if err != nil {
		return nil, xerrors.Errorf("scheduling worker: %w", err)
	}
	defer done()

	// TODO: select(candidateWorkers, ...)
	// TODO: remove the sectorbuilder abstraction, pass path directly
	return worker.SealCommit1(ctx, sector, ticket, seed, pieces, cids)
}

func (m *Manager) SealCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.Commit1Out) (proof storage.Proof, err error) {
	worker, done, err := m.getWorkerForSector(ctx, sector)
	if err != nil {
		return nil, xerrors.Errorf("scheduling worker: %w", err)
	}
	defer done()

	return worker.SealCommit2(ctx, sector, phase1Out)
}

func (m *Manager) FinalizeSector(ctx context.Context, sector abi.SectorID) error {
	worker, done, err := m.getWorkerForSector(ctx, sector)
	if err != nil {
		return xerrors.Errorf("scheduling worker: %w", err)
	}
	defer done()

	// TODO: Remove sector from sealing stores
	// TODO: Move the sector to long-term storage
	err = worker.FinalizeSector(ctx, sector)
	if err != nil {
		return err
	}

	err = m.sectorToWorkers.Delete(sector)
	return err
}

func (m *Manager) StorageLocal(ctx context.Context) (map[stores.ID]string, error) {
	l, err := m.localStore.Local(ctx)
	if err != nil {
		return nil, err
	}

	out := map[stores.ID]string{}
	for _, st := range l {
		out[st.ID] = st.LocalPath
	}

	return out, nil
}

func (m *Manager) FsStat(ctx context.Context, id stores.ID) (stores.FsStat, error) {
	return m.storage.FsStat(ctx, id)
}

func (m *Manager) Close() error {
	close(m.closing)
	return nil
}

var _ SectorManager = &Manager{}

// sectorToWorkers tracks which sector was processed by which worker
type sectorToWorkers struct {
	path string
	m    map[string]string

	mu sync.Mutex
}

func openSectorToWorkers(path string) (*sectorToWorkers, error) {
	_, err := os.Stat(path)

	if err != nil && os.IsNotExist(err) {
		return &sectorToWorkers{
			path: path,
			m:    make(map[string]string),
		}, nil
	}

	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var m map[string]string
	dec := json.NewDecoder(f)
	err = dec.Decode(&m)
	if err != nil {
		return nil, err
	}

	log.Infof("sector to workers map %#v\n", m)

	return &sectorToWorkers{
		path: path,
		m:    m,
	}, nil
}

func (s *sectorToWorkers) save() error {
	f, err := os.Create(s.path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(s.m)
}

func (s *sectorToWorkers) Put(sectorID abi.SectorID, workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := sectorID.Miner.String() + ":" + sectorID.Number.String()

	s.m[key] = workerID

	return s.save()
}

func (s *sectorToWorkers) Get(sectorID abi.SectorID) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := sectorID.Miner.String() + ":" + sectorID.Number.String()

	workerID, ok := s.m[key]
	return workerID, ok
}

func (s *sectorToWorkers) Delete(sectorID abi.SectorID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := sectorID.Miner.String() + ":" + sectorID.Number.String()
	delete(s.m, key)

	return s.save()
}

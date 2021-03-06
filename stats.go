package sectorstorage

import "github.com/filecoin-project/sector-storage/storiface"

func (m *Manager) WorkerStats() map[uint64]storiface.WorkerStats {
	m.workersLk.Lock()
	defer m.workersLk.Unlock()

	out := map[uint64]storiface.WorkerStats{}

	for id, handle := range m.workers {
		out[uint64(id)] = storiface.WorkerStats{
			Info:       handle.info,
			MemUsedMin: handle.memUsedMin,
			MemUsedMax: handle.memUsedMax,
			GpuUsed:    handle.gpuUsed,
			CpuUse:     handle.cpuUse,
		}
	}

	return out
}

package microvm

import "sort"

// snapshotView builds immutable external copies under the store lock.
func (s *Store) snapshotView() (map[string]VM, map[string]Operation, map[string]HostUsage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	vms := map[string]VM{}
	for id, rec := range s.state.VMs {
		vm := VM{
			Spec:      rec.Spec,
			State:     rec.State,
			HostID:    rec.HostID,
			CreatedAt: rec.CreatedAt,
			UpdatedAt: rec.UpdatedAt,
		}
		if snaps, ok := s.state.Snaps[id]; ok {
			ids := make([]string, 0, len(snaps))
			for sid := range snaps {
				ids = append(ids, sid)
			}
			sort.Strings(ids)
			for _, sid := range ids {
				vm.Snapshots = append(vm.Snapshots, snaps[sid].Snapshot)
			}
		}
		vms[id] = vm
	}

	ops := map[string]Operation{}
	for id, rec := range s.state.Ops {
		ops[id] = rec.Operation
	}

	return vms, ops, s.ledgerLocked()
}

func (s *Store) getVM(id string) (VM, bool) {
	vms, _, _ := s.snapshotView()
	vm, ok := vms[id]
	return vm, ok
}

func (s *Store) getOperation(id string) (Operation, bool) {
	_, ops, _ := s.snapshotView()
	op, ok := ops[id]
	return op, ok
}

// ReadBlock returns the content of one live disk block.
func (s *Store) ReadBlock(vmID string, index int) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	disk, ok := s.state.Blocks[vmID]
	if !ok {
		return nil, false
	}
	blobID, ok := disk[index]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), s.state.Blobs[blobID]...), true
}

// HostUsages returns the recomputed, explainable capacity ledger.
func (s *Store) HostUsages() []HostUsage {
	_, _, usages := s.snapshotView()
	out := make([]HostUsage, 0, len(usages))
	for _, u := range usages {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ledgerLocked computes usage purely from authoritative state, so the ledger
// can never drift from the VMs/snapshots that actually exist. When forPlace
// is true the CPU/memory columns count only running commitments (a stopped VM
// reserves disk but not CPU/memory), which is exactly the admission view.
// Caller must hold s.mu.
func (s *Store) ledgerLocked(forPlace ...bool) map[string]HostUsage {
	placement := len(forPlace) > 0 && forPlace[0]
	out := map[string]HostUsage{}
	for id, h := range s.state.Hosts {
		out[id] = HostUsage{
			ID:            id,
			CPUCapacity:   h.CPUCapacity,
			MemCapacity:   h.MemCapacity,
			DiskCapacity:  h.DiskCapacity,
			CPUOvercommit: s.state.Overcommit.CPU,
			MemOvercommit: s.state.Overcommit.Mem,
		}
	}

	// Live blocks never carry an extra charge: a placed VM reserves its full
	// declared spec disk from placement until release.
	for _, rec := range s.state.VMs {
		if rec.HostID == "" {
			continue // Pending, unplaced
		}
		u := out[rec.HostID]
		_ = placement // CPU/memory columns count running VMs in every view.
		switch rec.State {
		case StateStarting, StateRunning, StateStopping, StateRestarting:
			u.CPUUsed += rec.Spec.VCPU
			u.MemUsed += rec.Spec.Memory
		}
		u.DiskUsed += rec.Spec.Disk
		out[rec.HostID] = u
	}

	// Charge blob bytes kept alive only by snapshots (COW deltas). Accounting
	// is per host: identical content on two hosts is two physical copies.
	liveByHost := map[string]map[string]bool{}
	for _, rec := range s.state.VMs {
		if rec.HostID == "" {
			continue
		}
		set := liveByHost[rec.HostID]
		if set == nil {
			set = map[string]bool{}
			liveByHost[rec.HostID] = set
		}
		for _, blobID := range s.state.Blocks[rec.Spec.ID] {
			set[blobID] = true
		}
	}
	snapByHost := map[string]map[string]bool{}
	for vmID, snaps := range s.state.Snaps {
		rec := s.state.VMs[vmID]
		if rec == nil || rec.HostID == "" {
			continue
		}
		set := snapByHost[rec.HostID]
		if set == nil {
			set = map[string]bool{}
			snapByHost[rec.HostID] = set
		}
		for _, sr := range snaps {
			for _, blobID := range sr.Blocks {
				if blobID != "" {
					set[blobID] = true
				}
			}
		}
	}
	for host, set := range snapByHost {
		u := out[host]
		for blobID := range set {
			if liveByHost[host][blobID] {
				continue
			}
			u.DiskUsed += int64(len(s.state.Blobs[blobID]))
		}
		out[host] = u
	}
	return out
}

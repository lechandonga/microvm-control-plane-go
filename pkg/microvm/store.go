package microvm

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// CrashHook is invoked with every event immediately before it is fsynced. It
// is the single, deterministic crash seam used by restart-recovery tests:
// returning true aborts the process model before the event becomes durable.
type CrashHook func(ev Event) bool

type vmRec struct {
	Spec      Spec      `json:"spec"`
	State     VMState   `json:"state"`
	HostID    string    `json:"host_id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type snapRec struct {
	Snapshot
	// Blocks are the content ids captured point-in-time, per block index.
	Blocks []string `json:"blocks"`
}

type opRec struct {
	Operation
	IdemKey    string `json:"idem_key,omitempty"`
	Spec       Spec   `json:"spec,omitempty"`
	SnapshotID string `json:"snapshot_id,omitempty"`
	BlockIndex int    `json:"block_index,omitempty"`
	Data       []byte `json:"data,omitempty"`
}

// diskState is the authoritative in-memory projection of the WAL.
type diskState struct {
	Hosts      map[string]HostConfig          `json:"hosts"`
	Overcommit OvercommitRatio                `json:"overcommit"`
	VMs        map[string]*vmRec              `json:"vms"`
	Order      []string                       `json:"order"`
	Blocks     map[string]map[int]string      `json:"blocks"`
	Blobs      map[string][]byte              `json:"blobs"`
	Snaps      map[string]map[string]*snapRec `json:"snaps"`
	Ops        map[string]*opRec              `json:"ops"`
	Idem       map[string]string              `json:"idem"`
}

// Event is the union type of all durable mutations.
type Event struct {
	Kind string `json:"kind"`

	Spec       *Spec            `json:"spec,omitempty"`
	VMID       string           `json:"vm_id,omitempty"`
	HostID     string           `json:"host_id,omitempty"`
	State      VMState          `json:"state,omitempty"`
	Snapshot   *Snapshot        `json:"snapshot,omitempty"`
	SnapBlocks []string         `json:"snap_blocks,omitempty"`
	BlockIndex int              `json:"block_index,omitempty"`
	BlobID     string           `json:"blob_id,omitempty"`
	Data       []byte           `json:"data,omitempty"`
	Op         *opRec           `json:"op,omitempty"`
	IdemKey    string           `json:"idem_key,omitempty"`
	Hosts      []HostConfig     `json:"hosts,omitempty"`
	Overcommit *OvercommitRatio `json:"overcommit,omitempty"`
}

const (
	evInit        = "init"
	evVMDefined   = "vm_defined"
	evVMPlaced    = "vm_placed"
	evVMState     = "vm_state"
	evBlockWrite  = "block_write"
	evSnapCreated = "snap_created"
	evRollback    = "rolled_back"
	evSnapDeleted = "snap_deleted"
	evVMDestroyed = "vm_destroyed"
	evOpUpsert    = "op_upsert"
	evIdempotency = "idempotency"
)

var simulatedCrash = struct{ name string }{name: "simulated process crash"}

// Store is an fsynced write-ahead log with periodic full checkpoints. Every
// state change is an Event; the in-memory projection is only advanced after
// the event has been appended and fsynced, so a crash can never expose a
// mutation that the log does not contain.
type Store struct {
	mu     sync.Mutex
	dir    string
	f      *os.File
	w      *bufio.Writer
	seq    int64
	state  diskState
	crash  CrashHook
	closed bool
}

func newDiskState() diskState {
	return diskState{
		Hosts:  map[string]HostConfig{},
		VMs:    map[string]*vmRec{},
		Blocks: map[string]map[int]string{},
		Blobs:  map[string][]byte{},
		Snaps:  map[string]map[string]*snapRec{},
		Ops:    map[string]*opRec{},
		Idem:   map[string]string{},
	}
}

// OpenStore recovers (or initializes) a store rooted at dir. The pool
// configuration is only consumed on first creation; later restarts always
// trust the durable configuration so capacity can never change silently.
func OpenStore(dir string, hosts []HostConfig, ratio OvercommitRatio) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, state: newDiskState()}

	ckpt := filepath.Join(dir, "checkpoint.json")
	startSeq := int64(0)
	if b, err := os.ReadFile(ckpt); err == nil {
		var c struct {
			Seq   int64
			State diskState
		}
		if err := json.Unmarshal(b, &c); err != nil {
			return nil, fmt.Errorf("corrupt checkpoint: %w", err)
		}
		s.seq = c.Seq
		s.state = c.State
		startSeq = c.Seq
		if s.state.Hosts == nil {
			s.state = newDiskState()
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if f, err := os.Open(filepath.Join(dir, "wal.log")); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 64<<20)
		for sc.Scan() {
			line := sc.Bytes()
			if len(line) == 0 {
				continue
			}
			var rec struct {
				Seq int64
				Ev  json.RawMessage
			}
			if err := json.Unmarshal(line, &rec); err != nil {
				f.Close()
				return nil, fmt.Errorf("corrupt wal record: %w", err)
			}
			if rec.Seq <= startSeq {
				continue
			}
			var ev Event
			if err := json.Unmarshal(rec.Ev, &ev); err != nil {
				f.Close()
				return nil, err
			}
			if err := s.apply(ev); err != nil {
				f.Close()
				return nil, err
			}
			s.seq = rec.Seq
		}
		if err := sc.Err(); err != nil {
			f.Close()
			return nil, err
		}
		f.Close()
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if s.seq == 0 {
		for _, h := range hosts {
			if err := h.validate(); err != nil {
				return nil, err
			}
			s.state.Hosts[h.ID] = h
		}
		if ratio.CPU <= 0 {
			ratio.CPU = 1
		}
		if ratio.Mem <= 0 {
			ratio.Mem = 1
		}
		s.state.Overcommit = ratio
	}

	out, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.f = out
	s.w = bufio.NewWriter(out)

	if s.seq == 0 {
		if err := s.appendLocked(Event{Kind: evInit, Hosts: hosts, Overcommit: &ratio}); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// SetCrashHook installs the pre-fsync crash seam.
func (s *Store) SetCrashHook(h CrashHook) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.crash = h
}

func (s *Store) appendLocked(ev Event) error {
	if s.crash != nil && s.crash(ev) {
		// Simulated process death: nothing after this point may run, and the
		// event is deliberately never made durable.
		_ = s.f.Close()
		s.closed = true
		panic(simulatedCrash)
	}
	s.seq++
	b, err := json.Marshal(struct {
		Seq int64 `json:"seq"`
		Ev  Event `json:"ev"`
	}{Seq: s.seq, Ev: ev})
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if _, err := s.w.Write(b); err != nil {
		return err
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

// Apply durably commits ev and then advances the projection.
func (s *Store) Apply(ev Event) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	defer func() {
		if r := recover(); r != nil {
			if r == simulatedCrash {
				err = ErrClosed
				return
			}
			panic(r)
		}
	}()
	if err := s.appendLocked(ev); err != nil {
		return err
	}
	return s.apply(ev)
}

// PlaceAtomically admits and commits a placement in one locked step: the
// ledger recomputation, the capacity decision and the durable placement
// event cannot interleave, so concurrent defines can never oversell.
func (s *Store) PlaceAtomically(vmID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", ErrClosed
	}
	rec := s.state.VMs[vmID]
	if rec == nil {
		return "", errorf("%w: %q", ErrVMNotFound, vmID)
	}
	if rec.HostID != "" {
		return rec.HostID, nil
	}
	usages := s.ledgerLocked(true)
	ids := make([]string, 0, len(usages))
	for id := range usages {
		ids = append(ids, id)
	}
	sortStrings(ids)
	for _, id := range ids {
		u := usages[id]
		// Placement hard-reserves disk only. CPU/memory are admitted at Start,
		// so a stopped VM never holds compute capacity.
		if u.DiskFree() >= rec.Spec.Disk {
			if err := s.appendLocked(Event{Kind: evVMPlaced, VMID: vmID, HostID: id}); err != nil {
				return "", err
			}
			if err := s.apply(Event{Kind: evVMPlaced, VMID: vmID, HostID: id}); err != nil {
				return "", err
			}
			return id, nil
		}
	}
	return "", errorf("%w: no host has room for vm %q (vcpu=%d mem=%d disk=%d)",
		ErrCapacityExceeded, rec.Spec.ID, rec.Spec.VCPU, rec.Spec.Memory, rec.Spec.Disk)
}

func sortStrings(x []string) {
	for i := 1; i < len(x); i++ {
		for j := i; j > 0 && x[j-1] > x[j]; j-- {
			x[j-1], x[j] = x[j], x[j-1]
		}
	}
}

func (s *Store) apply(ev Event) error {
	now := time.Now().UTC()
	switch ev.Kind {
	case evInit:
		s.state.Hosts = map[string]HostConfig{}
		for _, h := range ev.Hosts {
			s.state.Hosts[h.ID] = h
		}
		if ev.Overcommit != nil {
			s.state.Overcommit = *ev.Overcommit
		}
	case evVMDefined:
		if _, ok := s.state.VMs[ev.Spec.ID]; ok {
			return nil
		}
		rec := &vmRec{Spec: *ev.Spec, State: StatePending, CreatedAt: now, UpdatedAt: now}
		s.state.VMs[rec.Spec.ID] = rec
		s.state.Order = append(s.state.Order, rec.Spec.ID)
		s.state.Blocks[rec.Spec.ID] = map[int]string{}
		s.state.Snaps[rec.Spec.ID] = map[string]*snapRec{}
	case evVMPlaced:
		rec := s.state.VMs[ev.VMID]
		if rec == nil {
			return fmt.Errorf("wal: placed unknown vm %q", ev.VMID)
		}
		rec.HostID = ev.HostID
		rec.State = StateStopped
		rec.UpdatedAt = now
	case evVMState:
		rec := s.state.VMs[ev.VMID]
		if rec == nil {
			return fmt.Errorf("wal: state change for unknown vm %q", ev.VMID)
		}
		rec.State = ev.State
		rec.UpdatedAt = now
	case evBlockWrite:
		disk := s.state.Blocks[ev.VMID]
		if _, ok := s.state.Blobs[ev.BlobID]; !ok {
			s.state.Blobs[ev.BlobID] = ev.Data
		}
		disk[ev.BlockIndex] = ev.BlobID
	case evSnapCreated:
		snaps := s.state.Snaps[ev.VMID]
		if _, ok := snaps[ev.Snapshot.ID]; ok {
			return nil
		}
		snaps[ev.Snapshot.ID] = &snapRec{Snapshot: *ev.Snapshot, Blocks: append([]string(nil), ev.SnapBlocks...)}
	case evRollback:
		sr, ok := s.state.Snaps[ev.VMID][ev.Snapshot.ID]
		if !ok {
			return fmt.Errorf("wal: rollback to unknown snapshot %q", ev.Snapshot.ID)
		}
		disk := map[int]string{}
		for idx, blob := range sr.Blocks {
			if blob != "" {
				disk[idx] = blob
			}
		}
		s.state.Blocks[ev.VMID] = disk
	case evSnapDeleted:
		delete(s.state.Snaps[ev.VMID], ev.Snapshot.ID)
	case evVMDestroyed:
		delete(s.state.VMs, ev.VMID)
		delete(s.state.Blocks, ev.VMID)
		delete(s.state.Snaps, ev.VMID)
		for i, id := range s.state.Order {
			if id == ev.VMID {
				s.state.Order = append(s.state.Order[:i], s.state.Order[i+1:]...)
				break
			}
		}
	case evOpUpsert:
		s.state.Ops[ev.Op.ID] = ev.Op
	case evIdempotency:
		s.state.Idem[ev.IdemKey] = ev.Op.ID
	default:
		return fmt.Errorf("wal: unknown event kind %q", ev.Kind)
	}
	return nil
}

// Checkpoint atomically writes a compacted snapshot and truncates the WAL.
func (s *Store) Checkpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	b, err := json.MarshalIndent(struct {
		Seq   int64
		State diskState
	}{Seq: s.seq, State: s.state}, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.dir, "checkpoint.tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, "checkpoint.json")); err != nil {
		return err
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	if err := s.f.Close(); err != nil {
		return err
	}
	nf, err := os.OpenFile(filepath.Join(s.dir, "wal.log"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := nf.Sync(); err != nil {
		return err
	}
	s.f = nf
	s.w = bufio.NewWriter(nf)
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Close flushes and releases the WAL.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Close()
}

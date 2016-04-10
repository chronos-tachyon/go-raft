package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	_ "github.com/golang/protobuf/proto"

	"github.com/chronos-tachyon/go-raft/internal/multierror"
	"github.com/chronos-tachyon/go-raft/internal/util"
	pb "github.com/chronos-tachyon/go-raft/proto"
)

var ErrMatchFail = errors.New("cannot match")

type Manager struct {
	// The storage that backs this Manager.
	// Does not change after construction.
	Storage Storage

	// The state machine that this Manager is managing.
	// Does not change after construction.
	Machine StateMachine

	MetadataVersion          uint64
	SnapshotVersion          uint64
	StartIndex               uint64
	BeforeStartTerm          uint64
	CurrentTerm              uint64
	VotedFor                 uint32
	ActiveConfiguration      *pb.Configuration
	CommitIndex              uint64
	CommittedConfigurationId uint64

	Entries     []*pb.LogEntry
	CommitHooks []CommitHook

	// The channel for scheduling closures to run on the execLoop thread.
	ExecChan chan func()

	// The waitgroup for blocking until background threads can shut down.
	wg sync.WaitGroup

	// The handle of the currently open log segment.
	seg *LogSegment
}

type CommitHook struct {
	Index    uint64
	Callback func(bool)
}

func NewManager(storage Storage, machine StateMachine, applyConfig func(*pb.Configuration)) *Manager {
	return &Manager{
		Storage: storage,
		Machine: machine,
	}
}

func (mgr *Manager) NextIndex() uint64 {
	mgr.checkInvariants()
	return mgr.StartIndex + uint64(len(mgr.Entries))
}

func (mgr *Manager) LatestIndex() uint64 {
	return mgr.NextIndex() - 1
}

func (mgr *Manager) At(index uint64) *pb.LogEntry {
	mgr.checkInvariants()
	lo := mgr.StartIndex
	hi := mgr.NextIndex()
	util.Assert(index >= lo && index < hi, "index=%d outside of [%d,%d)", index, lo, hi)
	e := mgr.Entries[index-mgr.StartIndex]
	util.Assert(e.Index == index, "index mismatch: expected %d, got %d", index, e.Index)
	return e
}

func (mgr *Manager) Term(index uint64) uint64 {
	mgr.checkInvariants()
	lo := mgr.StartIndex - 1
	hi := mgr.NextIndex()
	util.Assert(index >= lo && index < hi, "index=%d outside of [%d,%d)", index, lo, hi)
	if index == lo {
		return mgr.BeforeStartTerm
	}
	return mgr.Entries[index-mgr.StartIndex].Term
}

func (mgr *Manager) Slice(first, limit uint64) []*pb.LogEntry {
	mgr.checkInvariants()
	lo := mgr.StartIndex
	hi := mgr.NextIndex()
	util.Assert(first >= lo, "first=%d < lo=%d", first, lo)
	util.Assert(limit <= hi, "limit=%d > hi=%d", limit, hi)
	util.Assert(limit >= first, "limit=%d < first=%d", limit, first)
	return mgr.Entries[first-lo : limit-lo]
}

func (mgr *Manager) String() string {
	mgr.checkInvariants()
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "{%s} M%d S%d %d|%d:%02d:%02d|%d v%d [", mgr.Machine,
		mgr.MetadataVersion, mgr.SnapshotVersion, mgr.BeforeStartTerm,
		mgr.StartIndex, mgr.CommitIndex, mgr.NextIndex(),
		mgr.CurrentTerm, mgr.VotedFor)
	for _, e := range mgr.Entries {
		ch := e.Type.Char()
		if mgr.CommitIndex >= e.Index {
			ch = e.Type.UpperChar()
		}
		fmt.Fprintf(&buf, "%d%c,", e.Term, ch)
	}
	if slice := buf.Bytes(); slice[len(slice)-1] == ',' {
		buf.Truncate(buf.Len() - 1)
	}
	buf.WriteByte(']')
	return buf.String()
}

func (mgr *Manager) Open() error {
	mgr.checkInvariants()

	var meta pb.Metadata
	if err := LoadMetadata(mgr.Storage, &meta); err != nil {
		return err
	}

	snapshot, err := LoadSnapshot(mgr.Storage, meta.SnapshotVersion)
	if err != nil {
		return err
	}

	segments, err := ScanLogSegments(mgr.Storage)
	if err != nil {
		return err
	}

	var entries []*pb.LogEntry
	for _, segment := range segments {
		if segment.LastIndex != 0 && segment.LastIndex < meta.StartIndex {
			continue
		}
		err = segment.Open()
		if err != nil {
			return err
		}
		for {
			entry, err := segment.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				segment.Close()
				return err
			}
			if entry.Index >= meta.StartIndex {
				entries = append(entries, entry)
			}
		}
		err = segment.Close()
		if err != nil {
			return err
		}
	}

	seg := NewLogSegment(mgr.Storage, meta.StartIndex+uint64(len(entries)))
	if err := seg.Create(); err != nil {
		return err
	}

	mgr.Machine.Restore(snapshot)

	mgr.MetadataVersion = meta.MetadataVersion
	mgr.SnapshotVersion = meta.SnapshotVersion
	mgr.StartIndex = meta.StartIndex
	mgr.BeforeStartTerm = meta.BeforeStartTerm
	mgr.CurrentTerm = meta.CurrentTerm
	mgr.VotedFor = meta.VotedFor
	mgr.ActiveConfiguration = meta.Configuration

	mgr.CommitIndex = mgr.StartIndex - 1
	mgr.CommittedConfigurationId = 0
	mgr.Entries = entries

	mgr.ExecChan = make(chan func(), 8)
	mgr.seg = seg

	mgr.checkInvariants()

	mgr.wg.Add(1)
	go mgr.execLoop()
	return nil
}

func (mgr *Manager) AdvanceLog() error {
	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	mgr.ExecChan <- func() {
		if mgr.seg.NextIndex > mgr.seg.FirstIndex {
			newseg := NewLogSegment(mgr.Storage, mgr.seg.NextIndex)
			err = newseg.Create()
			if err == nil {
				err = mgr.seg.Close()
				mgr.seg = newseg
			}
		}
		wg.Done()
	}
	wg.Wait()
	return err
}

func (mgr *Manager) Close() error {
	mgr.checkInvariants()
	close(mgr.ExecChan)
	mgr.wg.Wait()
	err := mgr.seg.Close()
	*mgr = Manager{
		Storage: mgr.Storage,
		Machine: mgr.Machine,
	}
	mgr.checkInvariants()
	return err
}

func (mgr *Manager) Append(entries []*pb.LogEntry, donefn func(start, end uint64, err error)) {
	mgr.checkInvariants()
	next := mgr.NextIndex()
	dupe := make([]*pb.LogEntry, len(entries))
	for i, e := range entries {
		dupe[i] = &pb.LogEntry{
			Index:   next + uint64(i),
			Term:    e.Term,
			Type:    e.Type,
			Payload: e.Payload,
		}
	}
	mgr.Entries = append(mgr.Entries, dupe...)
	mgr.ExecChan <- func() {
		_, err := mgr.seg.Append(dupe)
		go donefn(next, next+uint64(len(entries)), err)
	}
}

func (mgr *Manager) OnCommit(index uint64, callback func(bool)) {
	mgr.checkInvariants()
	if mgr.CommitIndex >= index {
		go callback(true)
		return
	}
	mgr.CommitHooks = append(mgr.CommitHooks, CommitHook{
		Index:    index,
		Callback: callback,
	})
}

func (mgr *Manager) CommitTo(newCommitIndex uint64) {
	mgr.checkInvariants()
	util.Assert(newCommitIndex >= mgr.CommitIndex, "cannot undo commits: %d is less than %d", newCommitIndex, mgr.CommitIndex)
	util.Assert(newCommitIndex < mgr.NextIndex(), "cannot commit future log entries: %d is beyond %d", newCommitIndex, mgr.LatestIndex())

	for mgr.CommitIndex < newCommitIndex {
		mgr.CommitIndex++
		e := mgr.Entries[mgr.CommitIndex-mgr.StartIndex]
		switch e.Type {
		case pb.LogEntry_NOP:
			// pass
		case pb.LogEntry_COMMAND:
			mgr.Machine.Apply(e.Payload)
		case pb.LogEntry_CONFIGURATION:
			var cfg pb.Configuration
			util.MustUnmarshalProto(e.Payload, &cfg)
			mgr.CommittedConfigurationId = cfg.Id
		default:
			panic(fmt.Errorf("unknown LogEntry.Type %d", e.Type))
		}
	}

	mgr.checkInvariants()

	// Find any affected CommitHooks and tell them their entry was committed.
	var satisfied []func(bool)
	var unsatisfied []CommitHook
	for _, hook := range mgr.CommitHooks {
		if mgr.CommitIndex >= hook.Index {
			satisfied = append(satisfied, hook.Callback)
		} else {
			unsatisfied = append(unsatisfied, hook)
		}
	}
	mgr.CommitHooks = unsatisfied
	go (func() {
		for _, fn := range satisfied {
			fn(true)
		}
	})()
}

func (mgr *Manager) Match(prevIndex, prevTerm uint64, entries []*pb.LogEntry, donefn func(error)) {
	mgr.checkInvariants()
	firstIndex := prevIndex + 1

	// -IF-
	// {..........}
	//            {....}
	// No overlap; cannot match.  (Need at least prevTerm to match.)
	if firstIndex > mgr.NextIndex() {
		go donefn(ErrMatchFail)
		return
	}

	// -BEFORE-
	//    {..........}       {..........}
	// {....}           {....}
	for firstIndex < mgr.StartIndex && len(entries) > 0 {
		firstIndex++
		prevTerm = entries[0].Term
		entries = entries[1:]
	}
	// -AFTER-
	//    {..........}       {..........}
	// ___{.}           ____{}

	// -IF-
	//      {..........}
	// ____{}
	// No overlap; cannot match.
	if firstIndex < mgr.StartIndex {
		go donefn(ErrMatchFail)
		return
	}

	// -IF-
	// x{..........}  {...x......}
	// y{....}            y{....}
	// Mismatched term
	if mgr.Term(firstIndex-1) != prevTerm {
		go donefn(ErrMatchFail)
		return
	}

	lastCommittedIndex := util.Min(firstIndex+uint64(len(entries)), mgr.CommitIndex)

	// -BEFORE-
	// {CCCCCC....}
	//     {....}
	for firstIndex <= lastCommittedIndex {
		d := entries[0]
		e := mgr.Entries[firstIndex-mgr.StartIndex]
		// -IF-
		// {CCCCCC....}
		//     {DD..}
		// Mismatched term
		if d.Term != e.Term {
			go donefn(ErrMatchFail)
			return
		}
		util.Assert(d.Type == e.Type, "mismatched type: %d vs %d", d.Type, e.Type)
		util.Assert(bytes.Equal(d.Payload, e.Payload), "mismatched payload")
		firstIndex++
		entries = entries[1:]
	}
	// -AFTER-
	// {CCCCCC....}
	//     __{..}

	// -BEFORE-
	// {CCCCCCxxyy}
	//       {xxzz}
	for len(entries) > 0 && firstIndex < mgr.NextIndex() {
		d := entries[0]
		e := mgr.Entries[firstIndex-mgr.StartIndex]
		if d.Term != e.Term {
			break
		}
		util.Assert(d.Type == e.Type, "mismatched type: %d vs %d", d.Type, e.Type)
		util.Assert(bytes.Equal(d.Payload, e.Payload), "mismatched payload")
		firstIndex++
		entries = entries[1:]
	}
	// -AFTER-
	// {CCCCCCxxyy}
	//       __{zz}

	// Throw away all non-matching non-committed entries
	// -BEFORE-
	// {CCCCCCxxyy}
	//         {zz}
	mgr.Entries = mgr.Entries[0 : firstIndex-mgr.StartIndex]
	mgr.checkInvariants()
	// -AFTER-
	// {CCCCCCxx}
	//         {zz}

	// Find any affected CommitHooks and tell them their entry was rejected.
	var satisfied []func(bool)
	var unsatisfied []CommitHook
	for _, hook := range mgr.CommitHooks {
		if hook.Index >= firstIndex {
			satisfied = append(satisfied, hook.Callback)
		} else {
			unsatisfied = append(unsatisfied, hook)
		}
	}
	mgr.CommitHooks = unsatisfied
	go (func() {
		for _, fn := range satisfied {
			fn(false)
		}
	})()

	// Append the new entries
	mgr.Append(entries, func(_, _ uint64, err error) {
		donefn(err)
	})
}

func (mgr *Manager) TakeSnapshot(donefn func(error)) {
	mgr.checkInvariants()
	mgr.SnapshotVersion++
	mgr.MetadataVersion++
	meta := &pb.Metadata{
		MetadataVersion: mgr.MetadataVersion,
		SnapshotVersion: mgr.SnapshotVersion,
		StartIndex:      mgr.StartIndex,
		BeforeStartTerm: mgr.BeforeStartTerm,
		CurrentTerm:     mgr.CurrentTerm,
		VotedFor:        mgr.VotedFor,
	}
	snapshot := mgr.Machine.Snapshot()
	mgr.ExecChan <- func() {
		err := SaveSnapshot(mgr.Storage, meta.SnapshotVersion, snapshot)
		if err == nil {
			err = SaveMetadata(mgr.Storage, meta)
		}
		if err == nil {
			// TODO: trim logs
		}
		go donefn(err)
	}
}

func (mgr *Manager) UpdateTerm(term uint64, vote uint32, donefn func(error)) {
	mgr.checkInvariants()
	util.Assert(term >= mgr.CurrentTerm, "attempted to roll back from term %d to term %d", mgr.CurrentTerm, term)
	util.Assert(term != mgr.CurrentTerm || mgr.VotedFor == 0 || mgr.VotedFor == vote, "attempting to change vote from server %d to server %d", mgr.VotedFor, vote)
	changed := (mgr.CurrentTerm != term || mgr.VotedFor != vote)
	if changed {
		mgr.CurrentTerm = term
		mgr.VotedFor = vote
		mgr.MetadataVersion++
		meta := &pb.Metadata{
			MetadataVersion: mgr.MetadataVersion,
			SnapshotVersion: mgr.SnapshotVersion,
			StartIndex:      mgr.StartIndex,
			BeforeStartTerm: mgr.BeforeStartTerm,
			CurrentTerm:     mgr.CurrentTerm,
			VotedFor:        mgr.VotedFor,
		}
		mgr.ExecChan <- func() {
			err := SaveMetadata(mgr.Storage, meta)
			go donefn(err)
		}
	} else {
		go donefn(nil)
	}
}

func (mgr *Manager) Wipe() error {
	mgr.checkInvariants()
	var errs []error
	errs = append(errs, mgr.Close())
	mgr.Machine.Restore(nil)
	mgr.MetadataVersion = 0
	mgr.SnapshotVersion = 0
	mgr.StartIndex = 1
	mgr.CommitIndex = 0
	mgr.BeforeStartTerm = 0
	mgr.CurrentTerm = 0
	mgr.VotedFor = 0
	mgr.Entries = nil
	mgr.CommitHooks = nil
	var wg sync.WaitGroup
	wg.Add(1)
	mgr.ExecChan <- func() {
		names, err := mgr.Storage.List()
		errs = append(errs, err)
		for _, name := range names {
			errs = append(errs, mgr.Storage.Delete(name))
		}
		errs = append(errs, mgr.Open())
		wg.Done()
	}
	wg.Wait()
	return multierror.New(errs)
}

func (mgr *Manager) execLoop() {
	for {
		fn := <-mgr.ExecChan
		if fn == nil {
			break
		}
		fn()
	}
	mgr.wg.Done()
}

func (mgr *Manager) checkInvariants() {
	util.Assert(mgr.Storage != nil, "must have Storage")
	util.Assert(mgr.Machine != nil, "must have StateMachine")
	if mgr.seg != nil {
		ni := mgr.StartIndex + uint64(len(mgr.Entries))
		util.Assert(mgr.StartIndex > 0, "uninitialized StartIndex")
		util.Assert(mgr.CommitIndex >= mgr.StartIndex-1, "past CommitIndex %d (have %d:%d)", mgr.CommitIndex, mgr.StartIndex, ni)
		util.Assert(mgr.CommitIndex < ni, "future CommitIndex %d (have %d:%d)", mgr.CommitIndex, mgr.StartIndex, ni)
		for i, e := range mgr.Entries {
			index := mgr.StartIndex + uint64(i)
			util.Assert(e.Index == index, "index mismatch: expected %d, got %d", index, e.Index)
			util.Assert(e.Term <= mgr.CurrentTerm, "future log entry: expected term <= %d, got term %d", mgr.CurrentTerm, e.Term)
		}
	}
}

package draft

import (
	"fmt"
	"go.etcd.io/etcd/draft/collector"
	"go.etcd.io/etcd/raft/raftpb"
	"math/rand"
	"os"
	"reflect"
	"sync"
)

type mockingIMF struct {
	read  func(readFromIdx int) []raftpb.Message
	write func(message *raftpb.Message) error
}

func (m *mockingIMF) WriteIMF(message *raftpb.Message) error {
	return m.write(message)
}

func (m *mockingIMF) ReadIMF(readFromIdx int) ([]raftpb.Message, error) {
	return m.read(readFromIdx), nil
}

type mockingIMFStorage struct {
	m *sync.Mutex

	messages map[string][]raftpb.Message
	locks    map[string]*sync.RWMutex
}

func newMockingIMFStorage() *mockingIMFStorage {
	return &mockingIMFStorage{
		m:        &sync.Mutex{},
		messages: make(map[string][]raftpb.Message),
		locks:    make(map[string]*sync.RWMutex),
	}
}

func (s *mockingIMFStorage) OfferReadGrant(name string) IMFReader {
	s.m.Lock()
	defer s.m.Unlock()

	if _, ok := s.messages[name]; !ok {
		s.messages[name] = make([]raftpb.Message, 0, 10)
		s.locks[name] = &sync.RWMutex{}
	}

	return &mockingIMF{
		read: func(readFromIdx int) []raftpb.Message {
			l := s.locks[name]
			l.RLock()
			defer l.RUnlock()

			return s.messages[name][readFromIdx:]
		},
		write: func(message *raftpb.Message) error {
			return os.ErrPermission
		},
	}
}

func (s *mockingIMFStorage) OfferRedirectedReadGrant(name string, redirect func(int) []raftpb.Message) IMFReader {
	s.m.Lock()
	defer s.m.Unlock()

	if _, ok := s.messages[name]; !ok {
		s.messages[name] = make([]raftpb.Message, 0, 10)
		s.locks[name] = &sync.RWMutex{}
	}

	return &mockingIMF{
		read: redirect,
		write: func(message *raftpb.Message) error {
			return os.ErrPermission
		},
	}
}

func (s *mockingIMFStorage) OfferWriteGrant(name string) IMFWriter {
	s.m.Lock()
	defer s.m.Unlock()

	if _, ok := s.messages[name]; !ok {
		s.messages[name] = make([]raftpb.Message, 0, 10)
		s.locks[name] = &sync.RWMutex{}
	}

	return &mockingIMF{
		read: func(readFromIdx int) []raftpb.Message {
			return nil
		},
		write: func(message *raftpb.Message) error {
			l := s.locks[name]
			l.Lock()
			defer l.Unlock()

			s.messages[name] = append(s.messages[name], *message)
			return nil
		},
	}
}

func (s *mockingIMFStorage) OfferRedirectedWriteGrant(name string, redirect func(message *raftpb.Message) error) IMFWriter {
	s.m.Lock()
	defer s.m.Unlock()

	if _, ok := s.messages[name]; !ok {
		s.messages[name] = make([]raftpb.Message, 0, 10)
		s.locks[name] = &sync.RWMutex{}
	}

	return &mockingIMF{
		read: func(readFromIdx int) []raftpb.Message {
			return nil
		},
		write: redirect,
	}
}

type mockingIMFInjector struct {
	term     uint64
	from, to uint64

	prevTerm uint64
	index    uint64
	commit   uint64

	IMFWriter
}

func newMockingIMFInjector() *mockingIMFInjector {
	return &mockingIMFInjector{}
}

func (ij *mockingIMFInjector) String() string {
	if ij.IMFWriter != nil {
		return fmt.Sprintf("[INJECT][PREPARED][%v -> %v][t=%v][prevTerm=%v,prevIndex=%v]",
			ij.from, ij.to,
			ij.term,
			ij.prevTerm, ij.index-1,
		)
	}
	return "[INJECT][UNPREPARED]"
}

func (ij *mockingIMFInjector) Use(w IMFWriter) *mockingIMFInjector {
	ij.IMFWriter = w
	return ij
}

func (ij *mockingIMFInjector) AutoInit() *mockingIMFInjector {
	prev := rand.Uint64() & 0xffffffffffff
	td := rand.Uint64() & 0xff
	index := rand.Uint64()&0xffffffffffff + 1

	return ij.InitAs(prev+td, rand.Uint64(), rand.Uint64(), prev, index)
}

func (ij *mockingIMFInjector) InitAs(term, from, to, prevTerm, index uint64) *mockingIMFInjector {
	ij.term = term
	ij.from = from
	ij.to = to
	ij.prevTerm = prevTerm
	ij.index = index

	return ij
}

func (ij *mockingIMFInjector) Jump(tDelta, ltDelta, liDelta uint64) *mockingIMFInjector {
	return ij.JumpTo(ij.term+tDelta, ij.term+ltDelta, ij.index+liDelta)
}

func (ij *mockingIMFInjector) JumpTo(term, lastTerm, lastIndex uint64) *mockingIMFInjector {
	ij.term = term
	ij.prevTerm = lastTerm
	ij.index = lastIndex + 1

	return ij
}

func (ij *mockingIMFInjector) AutoVote() *mockingIMFInjector {
	td := rand.Uint64() & 0xff
	return ij.Vote(td)
}

func (ij *mockingIMFInjector) Vote(tDelta uint64) *mockingIMFInjector {
	ij.term += tDelta
	ij.IMFWriter.WriteIMF(&raftpb.Message{
		Type:    raftpb.MsgVote,
		To:      ij.to,
		From:    ij.from,
		Term:    ij.term,
		LogTerm: ij.prevTerm,
		Index:   ij.index,
	})

	return ij
}

func (ij *mockingIMFInjector) AutoAppend() *mockingIMFInjector {
	ij.IMFWriter.WriteIMF(&raftpb.Message{
		Type:    raftpb.MsgApp,
		To:      ij.to,
		From:    ij.from,
		Term:    ij.term,
		LogTerm: ij.prevTerm,
		Index:   ij.index - 1,
		Entries: []raftpb.Entry{{
			Term:  ij.term,
			Index: ij.index,
			Type:  raftpb.EntryNormal,
			Data:  []byte("AUTO_INJECTED"),
		}},
		Commit: ij.index - 1,
	})
	ij.index++
	ij.prevTerm = ij.term
	ij.commit = ij.index - 1

	return ij
}

func (ij *mockingIMFInjector) Append(commit, logTerm, logIndex uint64, entries []raftpb.Entry) *mockingIMFInjector {
	if len(entries) == 0 {
		if ij.term < logTerm {
			ij.term = logTerm
		}
		ij.IMFWriter.WriteIMF(&raftpb.Message{
			Type:    raftpb.MsgHeartbeat,
			To:      ij.to,
			From:    ij.from,
			Term:    ij.term,
			LogTerm: logTerm,
			Index:   logIndex,
			Commit:  commit,
		})
		ij.prevTerm = logTerm
	} else {
		if ij.term < entries[len(entries)-1].Term {
			ij.term = entries[len(entries)-1].Term
		}
		ij.IMFWriter.WriteIMF(&raftpb.Message{
			Type:    raftpb.MsgApp,
			To:      ij.to,
			From:    ij.from,
			Term:    ij.term,
			LogTerm: logTerm,
			Index:   logIndex,
			Entries: entries,
			Commit:  commit,
		})
		ij.prevTerm = entries[len(entries)-1].Term
	}

	ij.index = logIndex + uint64(len(entries)) + 1
	ij.commit = commit

	return ij
}

func (ij *mockingIMFInjector) AutoCommit() *mockingIMFInjector {
	ij.commit = ij.index - 1
	return ij.Commit(0)
}

func (ij *mockingIMFInjector) Commit(delta uint64) *mockingIMFInjector {
	max := ij.index - ij.commit - 1
	if delta > max {
		delta = max
	}
	ij.commit += delta

	ij.IMFWriter.WriteIMF(&raftpb.Message{
		Type:    raftpb.MsgHeartbeat,
		To:      ij.to,
		From:    ij.from,
		Term:    ij.term,
		LogTerm: ij.prevTerm,
		Index:   ij.index - 1,
		Commit:  ij.commit,
	})
	return ij
}

type mockingIMFInjectorRec struct {
	buf []*raftpb.Message
}

func (h *mockingIMFInjectorRec) Append(m *raftpb.Message) {
	h.buf = append(h.buf, m)
}

func (h *mockingIMFInjectorRec) GetAll() []*raftpb.Message {
	return h.buf
}

func (h *mockingIMFInjectorRec) GetTop() *raftpb.Message {
	if len(h.buf) == 0 {
		return nil
	}
	return h.buf[len(h.buf)-1]
}

func (h *mockingIMFInjectorRec) Clear() {
	if len(h.buf) == 0 {
		return
	}
	h.buf = h.buf[:0]
}

type mockingMemorableIMFInjectorWriter struct {
	w   IMFWriter
	his *mockingIMFInjectorRec
}

func (mw *mockingMemorableIMFInjectorWriter) WriteIMF(message *raftpb.Message) error {
	mw.his.Append(message)
	return mw.w.WriteIMF(message)
}

type mockingMemorableIMFInjector struct {
	*mockingIMFInjector
	history *mockingIMFInjectorRec
}

func newMockingMemorableIMFInjector(history *mockingIMFInjectorRec) *mockingMemorableIMFInjector {
	return &mockingMemorableIMFInjector{newMockingIMFInjector(), history}
}

func (injector *mockingMemorableIMFInjector) UseMemorable(w IMFWriter) *mockingIMFInjector {
	w = &mockingMemorableIMFInjectorWriter{w, injector.history}
	return injector.Use(w)
}

type mockingEntrySplitter struct {
	logTerm  uint64
	logIndex uint64
	ent      []raftpb.Entry

	entLen       int
	nextStartIdx int
	progress     int
	commitUpTo   int

	enableDispatch bool
	dispatched     []bool
}

func (mes *mockingEntrySplitter) SetDispatchService(enableDispatch bool) {
	mes.enableDispatch = enableDispatch
}

func newMockingEntrySplitter(logTerm uint64, logIndex uint64, ent []raftpb.Entry) *mockingEntrySplitter {
	return &mockingEntrySplitter{
		logTerm:        logTerm,
		logIndex:       logIndex,
		ent:            ent,
		entLen:         len(ent),
		commitUpTo:     -1,
		enableDispatch: true,
		dispatched:     make([]bool, len(ent)),
	}
}

func (mes *mockingEntrySplitter) Next(progressPromote int) (uint64, uint64, uint64, []raftpb.Entry) {
	var commit uint64
	if mes.commitUpTo == -1 {
		commit = mes.logIndex
	} else {
		commit = mes.ent[mes.commitUpTo].Index
	}

	if mes.nextStartIdx == mes.entLen {
		last := mes.ent[mes.entLen-1]
		return commit, last.Term, last.Index, nil
	}

	var lt uint64
	var li uint64

	if mes.nextStartIdx == 0 {
		lt = mes.logTerm
		li = mes.logIndex
	} else {
		last := mes.ent[mes.nextStartIdx-1]
		lt = last.Term
		li = last.Index
	}

	start := mes.nextStartIdx
	mes.progress += progressPromote
	if mes.progress > mes.entLen {
		mes.progress = mes.entLen
	}

	mes.nextStartIdx = mes.progress

	if mes.enableDispatch && start < mes.nextStartIdx {
		for i := start; i < mes.nextStartIdx; i++ {
			mes.dispatched[i] = true
		}
	}

	return commit, lt, li, mes.ent[start:mes.nextStartIdx]
}

func (mes *mockingEntrySplitter) NextTrivial() (uint64, uint64, uint64, []raftpb.Entry) {
	return mes.Next(0)
}

func (mes *mockingEntrySplitter) NextOneStep() (uint64, uint64, uint64, []raftpb.Entry) {
	return mes.Next(1)
}

func (mes *mockingEntrySplitter) NextAll() (uint64, uint64, uint64, []raftpb.Entry) {
	return mes.Next(mes.entLen)
}

func (mes *mockingEntrySplitter) CommitForwards(delta int) int {
	commit := mes.commitUpTo
	mes.commitUpTo += delta

	if mes.commitUpTo >= mes.progress {
		mes.commitUpTo = mes.progress - 1
	}

	return mes.commitUpTo - commit
}

func (mes *mockingEntrySplitter) CommitMost() {
	mes.CommitForwards(mes.entLen)
}

func (mes *mockingEntrySplitter) IsCommitAll() bool {
	return mes.commitUpTo == mes.entLen-1
}

func (mes *mockingEntrySplitter) IsDispatchAll() bool {
	if mes.entLen == 0 {
		return true
	}

	for _, dispatch := range mes.dispatched {
		if !dispatch {
			return false
		}
	}

	return true
}

func (mes *mockingEntrySplitter) MoveBackwards(step int) int {
	start := mes.nextStartIdx
	mes.nextStartIdx -= step
	if mes.nextStartIdx < 0 {
		mes.nextStartIdx = 0
	}
	return start - mes.nextStartIdx
}

func (mes *mockingEntrySplitter) MoveBackToZero() {
	mes.MoveBackwards(mes.entLen)
}

func (mes *mockingEntrySplitter) EquivEntrySeq(o ...interface{}) bool {
	if len(o) == 1 {
		switch o[0].(type) {
		case *mockingEntrySplitter:
			m := o[0].(*mockingEntrySplitter)
			return m.logTerm == mes.logTerm && m.logIndex == mes.logIndex && reflect.DeepEqual(m.ent, mes.ent)
		case *collector.EntryFragment:
			f := o[0].(*collector.EntryFragment)
			return f.LogTerm == mes.logTerm && f.LogIndex == mes.logIndex && reflect.DeepEqual(f.Fragment, mes.ent)
		case collector.ConsecutiveEntryCollector:
			c := o[0].(collector.ConsecutiveEntryCollector)
			_, ent, lt, li := c.FetchAllEntries()
			return mes.EquivEntrySeq(lt, li, ent)
		case *collector.MultiFragmentsCollector:
			c := o[0].(*collector.MultiFragmentsCollector)
			ok, fs := c.FetchAllFragments()
			if !ok || len(fs) != 1 {
				return false
			}
			return mes.EquivEntrySeq(fs[0])
		}
	} else if len(o) == 3 {
		logTerm := o[0].(uint64)
		logIndex := o[1].(uint64)
		ent := o[2].([]raftpb.Entry)

		return logTerm == mes.logTerm && logIndex == mes.logIndex && reflect.DeepEqual(ent, mes.ent)
	}

	panic("illegal arguments")
}

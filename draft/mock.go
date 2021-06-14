package draft

import (
	"fmt"
	"go.etcd.io/etcd/raft/raftpb"
	"math/rand"
	"os"
	"sync"
)

//mockingIMF is a generic implementation of IMFReader and IMFWriter
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

//mockingIMFStorage provides mocking file system, offering implementation of IMFReader and IMFWriter on
// specific rack and file.
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

//OfferReadGrant offers an implementation of IMFReader to the specific file
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

//OfferWriteGrant offers an implementation of IMFWriter to the specific file
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

//mockingIMFInjector injects Raft messages through IMFWriter and provides an easy way of generating those messages.
// To be mentioned, it does not verify the rationality of the messages.
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
			Type:   raftpb.MsgHeartbeat,
			To:     ij.to,
			From:   ij.from,
			Term:   ij.term,
			Commit: commit,
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

//mockingIMFInjectorRec lists out the record of generated messages.
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

//mockingMemorableIMFInjectorWriter wraps IMFWriter of mockingIMFInjector and records each message.
type mockingMemorableIMFInjectorWriter struct {
	w   IMFWriter
	his *mockingIMFInjectorRec
}

func (mw *mockingMemorableIMFInjectorWriter) WriteIMF(message *raftpb.Message) error {
	mw.his.Append(message)
	return mw.w.WriteIMF(message)
}

//mockingMemorableIMFInjector wraps mockingIMFInjector with a mockingIMFInjectorRec.
// Every message generated by injector will be kept in record for further checking.
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

//mockingEntrySplitter splits an entry sequence into several dispatches. Each dispatch contains a consecutive
// subsequence of entries.
type mockingEntrySplitter struct {
	*entryComparator

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
	if len(ent) == 0 {
		panic("illegal arguments")
	}

	return &mockingEntrySplitter{
		entryComparator: newEntryComparator(logTerm, logIndex, ent),
		commitUpTo:      -1,
		enableDispatch:  true,
		dispatched:      make([]bool, len(ent)),
	}
}

//Next generates an entry sequence with index in between nextStartIdx(inclusive) and
// progress+progressPromote(exclusive), while updating internal fields.
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

//NextTrivial generates an entry sequence with index in between nextStartIdx(inclusive) and
// progress(exclusive), while updating internal fields.
func (mes *mockingEntrySplitter) NextTrivial() (uint64, uint64, uint64, []raftpb.Entry) {
	return mes.Next(0)
}

//NextOneStep generates an entry sequence with index in between nextStartIdx(inclusive) and
// progress+1(exclusive), while updating internal fields.
func (mes *mockingEntrySplitter) NextOneStep() (uint64, uint64, uint64, []raftpb.Entry) {
	return mes.Next(1)
}

//NextAll generates an entry sequence with index no smaller than nextStartIdx, while updating internal fields.
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

//IsDispatchAll checks if all entries have been dispatched before. To be mentioned, this function works if and only if
// enableDispatch is true throughout the calling.
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

//MoveBackwards decreases nextStartIndex to nextStartIndex-step.
func (mes *mockingEntrySplitter) MoveBackwards(step int) int {
	start := mes.nextStartIdx
	mes.nextStartIdx -= step
	if mes.nextStartIdx < 0 {
		mes.nextStartIdx = 0
	}
	return start - mes.nextStartIdx
}

//MoveBackToZero decreases nextStartIndex to 0.
func (mes *mockingEntrySplitter) MoveBackToZero() {
	mes.MoveBackwards(mes.entLen)
}

type mockingStorageWrapper struct {
	*mockingIMFStorage
	injectors map[string]*mockingIMFInjector
}

func newMockingStorageWrapper() *mockingStorageWrapper {
	return &mockingStorageWrapper{mockingIMFStorage: newMockingIMFStorage(), injectors: make(map[string]*mockingIMFInjector)}
}

func (sw *mockingStorageWrapper) Sender(from, to uint64) *mockingIMFInjector {
	token := rf2t(ft2rf(from, to))
	if _, ok := sw.injectors[token]; !ok {
		sw.injectors[token] = newMockingIMFInjector().Use(sw.OfferWriteGrant(token)).InitAs(0, from, to, 0, 0)
	}
	return sw.injectors[token]
}

func (sw *mockingStorageWrapper) Receiver(from, to uint64) IMFReader {
	token := rf2t(ft2rf(from, to))
	return sw.OfferReadGrant(token)
}

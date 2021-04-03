package draft

import (
	"fmt"
	"go.etcd.io/etcd/raft/raftpb"
	"math/rand"
	"os"
	"sync"
)

type mockingIMF struct {
	read  func(readFromIdx int) []raftpb.Message
	write func(message *raftpb.Message) error
}

func (m *mockingIMF) WriteIMF(message *raftpb.Message) error {
	return m.write(message)
}

func (m *mockingIMF) ReadIMF(readFromIdx int) []raftpb.Message {
	return m.read(readFromIdx)
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

func (s *mockingIMFStorage) OfferWriteGrant(name string) IMFWriter {
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
			l := s.locks[name]
			l.Lock()
			defer l.Unlock()

			s.messages[name] = append(s.messages[name], *message)
			return nil
		},
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

func (injector *mockingIMFInjector) String() string {
	if injector.IMFWriter != nil {
		return fmt.Sprintf("[INJECT][PREPARED][%v -> %v][t=%v][prevTerm=%v,prevIndex=%v]",
			injector.from, injector.to,
			injector.term,
			injector.prevTerm, injector.index-1,
		)
	}
	return "[INJECT][UNPREPARED]"
}

func (injector *mockingIMFInjector) Use(w IMFWriter) *mockingIMFInjector {
	injector.IMFWriter = w
	return injector
}

func (injector *mockingIMFInjector) AutoInit() *mockingIMFInjector {
	prev := rand.Uint64() & 0xffffffffffff
	td := rand.Uint64() & 0xff
	index := rand.Uint64()&0xffffffffffff + 1

	return injector.InitAs(prev+td, rand.Uint64(), rand.Uint64(), prev, index)
}

func (injector *mockingIMFInjector) InitAs(term, from, to, prevTerm, index uint64) *mockingIMFInjector {
	injector.term = term
	injector.from = from
	injector.to = to
	injector.prevTerm = prevTerm
	injector.index = index

	return injector
}

func (injector *mockingIMFInjector) Jump(tDelta, ltDelta, liDelta uint64) *mockingIMFInjector {
	return injector.JumpTo(injector.term+tDelta, injector.term+ltDelta, injector.index+liDelta)
}

func (injector *mockingIMFInjector) JumpTo(term, lastTerm, lastIndex uint64) *mockingIMFInjector {
	injector.term = term
	injector.prevTerm = lastTerm
	injector.index = lastIndex + 1

	return injector
}

func (injector *mockingIMFInjector) AutoVote() *mockingIMFInjector {
	td := rand.Uint64() & 0xff
	return injector.Vote(td)
}

func (injector *mockingIMFInjector) Vote(tDelta uint64) *mockingIMFInjector {
	injector.term += tDelta
	injector.IMFWriter.WriteIMF(&raftpb.Message{
		Type:    raftpb.MsgVote,
		To:      injector.to,
		From:    injector.from,
		Term:    injector.term,
		LogTerm: injector.prevTerm,
		Index:   injector.index,
	})

	return injector
}

func (injector *mockingIMFInjector) AutoAppend() *mockingIMFInjector {
	injector.IMFWriter.WriteIMF(&raftpb.Message{
		Type:    raftpb.MsgApp,
		To:      injector.to,
		From:    injector.from,
		Term:    injector.term,
		LogTerm: injector.prevTerm,
		Index:   injector.index - 1,
		Entries: []raftpb.Entry{{
			Term:  injector.term,
			Index: injector.index,
			Type:  raftpb.EntryNormal,
			Data:  []byte("AUTO_INJECTED"),
		}},
		Commit: injector.index - 1,
	})
	injector.index++
	injector.prevTerm = injector.term
	injector.commit = injector.index - 1

	return injector
}

func (injector *mockingIMFInjector) Append(commit, logTerm, logIndex uint64, entries []raftpb.Entry) *mockingIMFInjector {
	if len(entries) == 0 {
		injector.IMFWriter.WriteIMF(&raftpb.Message{
			Type:    raftpb.MsgHeartbeat,
			To:      injector.to,
			From:    injector.from,
			Term:    injector.term,
			LogTerm: logTerm,
			Index:   logIndex,
			Commit:  commit,
		})
		injector.prevTerm = logTerm
	} else {
		injector.IMFWriter.WriteIMF(&raftpb.Message{
			Type:    raftpb.MsgApp,
			To:      injector.to,
			From:    injector.from,
			Term:    injector.term,
			LogTerm: logTerm,
			Index:   logIndex,
			Entries: entries,
			Commit:  commit,
		})
		injector.prevTerm = entries[len(entries)-1].Term
	}

	injector.index = logIndex + uint64(len(entries)) + 1
	injector.commit = commit

	return injector
}

func (injector *mockingIMFInjector) AutoCommit() *mockingIMFInjector {
	injector.commit = injector.index - 1
	return injector.Commit(0)
}

func (injector *mockingIMFInjector) Commit(delta uint64) *mockingIMFInjector {
	max := injector.index - injector.commit - 1
	if delta > max {
		delta = max
	}
	injector.commit += delta

	injector.IMFWriter.WriteIMF(&raftpb.Message{
		Type:    raftpb.MsgHeartbeat,
		To:      injector.to,
		From:    injector.from,
		Term:    injector.term,
		LogTerm: injector.prevTerm,
		Index:   injector.index - 1,
		Commit:  injector.commit,
	})
	return injector
}

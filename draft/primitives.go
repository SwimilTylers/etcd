package draft

import (
	"go.etcd.io/etcd/draft/collector"
	"go.etcd.io/etcd/raft/raftpb"
	"os"
	"path/filepath"
)

type Update struct {
	SourceFile string
	Term       uint64

	ZeroDelta bool

	Collected collector.EntryFragmentCollector
	Commit    uint64
	VoteMsg   *raftpb.Message
	Err       error
}

func errUpdate(file string, err error) *Update {
	return &Update{
		SourceFile: file,
		Err:        err,
	}
}

func noUpdate(file string) *Update {
	return &Update{
		SourceFile: file,
		ZeroDelta:  true,
	}
}

func newUpdate(file string, term uint64, c collector.EntryFragmentCollector, commit uint64, vote *raftpb.Message) *Update {
	return &Update{
		SourceFile: file,
		Term:       term,
		ZeroDelta:  false,
		Collected:  c,
		Commit:     commit,
		VoteMsg:    vote,
	}
}

type SyncPrimitives interface {
	//Write persists draft message to the specific destination
	Write(rack, file string, message *raftpb.Message) error

	//GetUpdate fetches and categorizes the delta of the specific destination
	GetUpdate(rack, file string) *Update
}

type AsyncPrimitives interface {
	//AsyncWrite submits Write job and sends a signal to 'c' when the job is done.
	AsyncWrite(rack, file string, message *raftpb.Message, c chan<- bool) error

	//AsyncGetUpdate submits GetUpdate job and sends a signal to 'c' when the job is done.
	AsyncGetUpdate(rack, file string, c chan<- *Update) error
}

type Primitives interface {
	SyncPrimitives
	AsyncPrimitives
}

type updater struct {
	next   int
	reader IMFReader
}

func (u *updater) RecentIMF() []raftpb.Message {
	messages := u.reader.ReadIMF(u.next)
	u.next += len(messages)
	return messages
}

type PrimitiveProvider struct {
	reader    map[string]*updater
	writer    map[string]IMFWriter
	collector map[string]collector.EntryFragmentCollector
}

func NewPrimitiveProvider() *PrimitiveProvider {
	return &PrimitiveProvider{
		reader:    make(map[string]*updater),
		writer:    make(map[string]IMFWriter),
		collector: make(map[string]collector.EntryFragmentCollector),
	}
}

func (pvd *PrimitiveProvider) AsyncWrite(rack, file string, message *raftpb.Message, c chan<- bool) error {
	if w, ok := pvd.writer[filepath.Join(rack, file)]; ok {
		go func(message *raftpb.Message, c chan<- bool) {
			if err := w.WriteIMF(message); err != nil {
				c <- false
			} else {
				c <- true
			}
		}(message, c)

		return nil
	}

	return os.ErrNotExist
}

func (pvd *PrimitiveProvider) AsyncGetUpdate(rack, file string, c chan<- *Update) error {
	signature := filepath.Join(rack, file)
	if r, ok := pvd.reader[signature]; ok {
		ctr := pvd.collector[signature]

		go func(c chan<- *Update, f string, u *updater, cl collector.EntryFragmentCollector) {
			cl.Refresh()
			c <- pvd.getUpdate(f, u, cl)
		}(c, file, r, ctr)

		return nil
	}

	return os.ErrNotExist
}

func (pvd *PrimitiveProvider) Write(rack, file string, message *raftpb.Message) error {
	if w, ok := pvd.writer[filepath.Join(rack, file)]; ok {
		return w.WriteIMF(message)
	}

	return os.ErrNotExist
}

func (pvd *PrimitiveProvider) GetUpdate(rack, file string) *Update {
	if r, ok := pvd.reader[filepath.Join(rack, file)]; ok {
		var c = pvd.collector[filepath.Join(rack, file)]
		c.Refresh()
		return pvd.getUpdate(file, r, c)
	}

	return errUpdate(file, os.ErrNotExist)
}

func (pvd *PrimitiveProvider) IMFWriter(rack, file string) IMFWriter {
	key := filepath.Join(rack, file)
	return pvd.writer[key]
}

func (pvd *PrimitiveProvider) GrantWrite(rack, file string, val IMFWriter) {
	key := filepath.Join(rack, file)
	pvd.writer[key] = val
}

func (pvd *PrimitiveProvider) IMFReader(rack, file string) (IMFReader, int) {
	key := filepath.Join(rack, file)
	u := pvd.reader[key]
	return u.reader, u.next
}

func (pvd *PrimitiveProvider) EntryFragmentCollector(rack, file string) collector.EntryFragmentCollector {
	key := filepath.Join(rack, file)
	return pvd.collector[key]
}

func (pvd *PrimitiveProvider) GrantRead(rack, file string, val IMFReader, c collector.EntryFragmentCollector) {
	key := filepath.Join(rack, file)
	pvd.reader[key] = &updater{
		next:   0,
		reader: val,
	}
	pvd.collector[key] = c
}

func (pvd *PrimitiveProvider) ResetRead(rack, file string, idx int, cRefresh bool) bool {
	key := filepath.Join(rack, file)
	u, uok := pvd.reader[key]
	c, cok := pvd.collector[key]

	if uok && cok {
		u.next = idx
		if cRefresh {
			c.Refresh()
		}

		return true
	}

	return false
}

func (pvd *PrimitiveProvider) getUpdate(file string, r *updater, c collector.EntryFragmentCollector) *Update {
	messages := r.RecentIMF()

	if len(messages) == 0 {
		return noUpdate(file)
	}

	for _, m := range messages {
		if m.Type != raftpb.MsgApp && m.Type != raftpb.MsgHeartbeat {
			continue
		}

		c.AddEntriesWithSubmitter(m.Term, m.Entries, m.LogTerm, m.Index)
	}

	last := messages[len(messages)-1]
	var vote *raftpb.Message = nil

	if last.Type == raftpb.MsgVote {
		vote = &last
	}

	return newUpdate(file, last.Term, c, last.Commit, vote)
}

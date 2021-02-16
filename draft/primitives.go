package draft

import (
	"go.etcd.io/etcd/raft/raftpb"
	"os"
	"path/filepath"
)

type Update struct {
	SourceFile string
	Term       uint64

	ZeroDelta bool

	Entries []raftpb.Entry
	VoteMsg *raftpb.Message
	Err     error
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

func newUpdate(file string, term uint64, entries []raftpb.Entry, vote *raftpb.Message) *Update {
	return &Update{
		SourceFile: file,
		Term:       term,
		ZeroDelta:  false,
		Entries:    entries,
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

type PrimitiveProvider struct {
	reader map[string]struct {
		next   int
		reader IMFReader
	}
	writer map[string]IMFWriter

	cGet CollectorFactory
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
	if _, ok := pvd.reader[filepath.Join(rack, file)]; !ok {
		return os.ErrNotExist
	}

	go func(c chan<- *Update, cl Collector) {
		cl.Refresh()
		c <- pvd.getUpdate(rack, file, cl)
	}(c, pvd.cGet.GetCollector(rack, file))

	return nil
}

func (pvd *PrimitiveProvider) Write(rack, file string, message *raftpb.Message) error {
	if w, ok := pvd.writer[filepath.Join(rack, file)]; ok {
		return w.WriteIMF(message)
	}

	return os.ErrNotExist
}

func (pvd *PrimitiveProvider) GetUpdate(rack, file string) *Update {
	var c = pvd.cGet.GetCollector(rack, file)
	c.Refresh()
	return pvd.getUpdate(rack, file, c)
}

func (pvd *PrimitiveProvider) getUpdate(rack, file string, c Collector) *Update {
	if r, ok := pvd.reader[filepath.Join(rack, file)]; ok {
		messages := r.reader.ReadIMF(r.next)
		r.next += len(messages)

		if len(messages) == 0 {
			return noUpdate(file)
		}

		for _, m := range messages {
			c.AddEntries(m.Entries, m.LogTerm, m.Index)
		}

		last := messages[len(messages)-1]
		var vote *raftpb.Message = nil

		if last.Type == raftpb.MsgVote {
			vote = &last
		}

		ent, _, _ := c.DropAllEntries()

		return newUpdate(file, last.Term, ent, vote)
	}

	return errUpdate(file, os.ErrNotExist)
}

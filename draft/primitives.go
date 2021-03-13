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

	Collected Collector
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

func newUpdate(file string, term uint64, c Collector, vote *raftpb.Message) *Update {
	return &Update{
		SourceFile: file,
		Term:       term,
		ZeroDelta:  false,
		Collected:  c,
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
	collector map[string]Collector
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
		collector := pvd.collector[signature]

		go func(c chan<- *Update, f string, u *updater, cl Collector) {
			cl.Refresh()
			c <- pvd.getUpdate(f, u, cl)
		}(c, file, r, collector)

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

func (pvd *PrimitiveProvider) getUpdate(file string, r *updater, c Collector) *Update {
	messages := r.RecentIMF()

	if len(messages) == 0 {
		return noUpdate(file)
	}

	for _, m := range messages {
		if m.Type != raftpb.MsgApp && m.Type != raftpb.MsgHeartbeat {
			continue
		}

		c.AddEntries(m.Entries, m.LogTerm, m.Index)
	}

	last := messages[len(messages)-1]
	var vote *raftpb.Message = nil

	if last.Type == raftpb.MsgVote {
		vote = &last
	}

	return newUpdate(file, last.Term, c, vote)
}

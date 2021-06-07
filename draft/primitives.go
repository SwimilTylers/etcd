package draft

import (
	"go.etcd.io/etcd/draft/collector"
	"go.etcd.io/etcd/raft/raftpb"
	"os"
	"path/filepath"
)

type AEUpdate struct {
	Term   uint64
	Commit uint64
	AE     collector.EntryFragmentCollector
}

type Update struct {
	SourceFile string

	ZeroDelta bool

	App  *AEUpdate
	Vote *raftpb.Message

	Err error
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

func newUpdate(file string, app *AEUpdate, vote *raftpb.Message) *Update {
	return &Update{
		SourceFile: file,
		ZeroDelta:  false,
		App:        app,
		Vote:       vote,
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

type PreservablePrimitives interface {
	Primitives

	//Preserve holds the result of the last update
	Preserve(rack, file string, vote *raftpb.Message) error
}

type updater struct {
	next   int
	reader IMFReader
}

func (u *updater) RecentIMF() ([]raftpb.Message, error) {
	if messages, err := u.reader.ReadIMF(u.next); err != nil {
		return nil, err
	} else {
		u.next += len(messages)
		return messages, nil
	}
}

type efcInfo struct {
	efc        collector.EntryFragmentCollector
	refresh    bool
	prefixVote *raftpb.Message
}

type PrimitiveProvider struct {
	reader    map[string]*updater
	writer    map[string]IMFWriter
	collector map[string]*efcInfo
}

func NewPrimitiveProvider() *PrimitiveProvider {
	return &PrimitiveProvider{
		reader:    make(map[string]*updater),
		writer:    make(map[string]IMFWriter),
		collector: make(map[string]*efcInfo),
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

	return ErrWriterNotExist
}

func (pvd *PrimitiveProvider) AsyncGetUpdate(rack, file string, c chan<- *Update) error {
	signature := filepath.Join(rack, file)
	if r, ok := pvd.reader[signature]; ok {
		cInfo := pvd.collector[signature]
		var prefixVote *raftpb.Message = nil
		if cInfo.refresh {
			cInfo.efc.Refresh()
		} else {
			cInfo.refresh = true
			prefixVote = cInfo.prefixVote
			cInfo.prefixVote = nil
		}

		go func(c chan<- *Update, f string, u *updater, efc collector.EntryFragmentCollector, pVote *raftpb.Message) {
			c <- pvd.getUpdate(f, u, efc, pVote)
		}(c, file, r, cInfo.efc, prefixVote)

		return nil
	}

	return ErrReaderNotExist
}

func (pvd *PrimitiveProvider) Write(rack, file string, message *raftpb.Message) error {
	if w, ok := pvd.writer[filepath.Join(rack, file)]; ok {
		return w.WriteIMF(message)
	}

	return os.ErrNotExist
}

func (pvd *PrimitiveProvider) GetUpdate(rack, file string) *Update {
	key := filepath.Join(rack, file)
	if r, ok := pvd.reader[key]; ok {
		var cInfo = pvd.collector[key]
		var prefixVote *raftpb.Message = nil
		if cInfo.refresh {
			cInfo.efc.Refresh()
		} else {
			cInfo.refresh = true
			prefixVote = cInfo.prefixVote
			cInfo.prefixVote = nil
		}
		return pvd.getUpdate(file, r, cInfo.efc, prefixVote)
	}

	return errUpdate(file, os.ErrNotExist)
}

func (pvd *PrimitiveProvider) Preserve(rack, file string, vote *raftpb.Message) error {
	key := filepath.Join(rack, file)
	if cInfo, ok := pvd.collector[key]; ok {
		cInfo.refresh = false
		cInfo.prefixVote = vote

		return nil
	}
	return ErrReaderNotExist
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
	return pvd.collector[key].efc
}

func (pvd *PrimitiveProvider) GrantRead(rack, file string, val IMFReader, c collector.EntryFragmentCollector) {
	key := filepath.Join(rack, file)
	pvd.reader[key] = &updater{
		next:   0,
		reader: val,
	}
	pvd.collector[key] = &efcInfo{c, true, nil}
}

func (pvd *PrimitiveProvider) ResetRead(rack, file string, idx int, cRefresh bool) bool {
	key := filepath.Join(rack, file)
	u, uok := pvd.reader[key]
	c, cok := pvd.collector[key]

	if uok && cok {
		u.next = idx
		c.refresh = true
		if cRefresh {
			c.efc.Refresh()
		}
		c.prefixVote = nil

		return true
	}

	return false
}

func (pvd *PrimitiveProvider) getUpdate(file string, r *updater, c collector.EntryFragmentCollector, vote *raftpb.Message) *Update {
	messages, err := r.RecentIMF()

	if err != nil {
		return errUpdate(file, err)
	}

	if len(messages) == 0 {
		return noUpdate(file)
	}

	var app *AEUpdate

	for _, m := range messages {
		switch m.Type {
		case raftpb.MsgPreVote, raftpb.MsgVote:
			vote = &m
		case raftpb.MsgApp:
			vote = nil
			app = refreshAEUpdate(app, m.Term, m.Commit, c)
			c.AddEntriesWithSubmitter(m.Term, m.Entries, m.LogTerm, m.Index)
		case raftpb.MsgHeartbeat:
			vote = nil
			app = refreshAEUpdate(app, m.Term, m.Commit, c)
		}
	}

	return newUpdate(file, app, vote)
}

func refreshAEUpdate(app *AEUpdate, term, commit uint64, c collector.EntryFragmentCollector) *AEUpdate {
	if app == nil {
		app = &AEUpdate{Term: term, Commit: commit, AE: c}
	} else {
		if app.Term < term {
			app.Term = term
		}

		if app.Commit < commit {
			app.Commit = commit
		}
	}

	return app
}

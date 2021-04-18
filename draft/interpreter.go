package draft

import (
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

type Interpreter interface {
	//IsSupported checks if translation service is available to this Message.
	IsSupported(m *raftpb.Message) bool

	//Interpret takes in Raft Request and gives out Raft Response
	Interpret(m *raftpb.Message) *raftpb.Message
}

type OneToOneInterpreter struct {
	lg *zap.Logger

	racks    []string
	syncRack string
	p2r      map[uint64]string

	files []string
	p2f   map[uint64]string
	f2p   map[string]uint64

	drp Primitives
	an  map[string]RackProgressAnalyzer
}

func (itp *OneToOneInterpreter) IsSupported(m *raftpb.Message) bool {
	return m.Type == raftpb.MsgVote || m.Type == raftpb.MsgApp || m.Type == raftpb.MsgHeartbeat || m.Type == raftpb.MsgDRSync
}

func (itp *OneToOneInterpreter) Interpret(m *raftpb.Message) *raftpb.Message {
	switch m.Type {
	case raftpb.MsgVote:
		return itp.interpretVote(m)
	case raftpb.MsgApp:
		return itp.interpretApp(m)
	case raftpb.MsgHeartbeat:
		if r := itp.interpretApp(m); r != nil {
			r.Type = raftpb.MsgHeartbeatResp
			return r
		}
		break
	case raftpb.MsgDRSync:
		return itp.drSync()
	}

	return nil
}

func (itp *OneToOneInterpreter) drSync() *raftpb.Message {
	an := itp.an[itp.syncRack]

	zeroUpdate, _ := itp.getUpdatesFromOtherFiles(itp.syncRack, "", an)

	if zeroUpdate {
		return nil
	}

	progress, _ := an.Progress()

	if progress.NoProgress {
		return nil
	}

	return &raftpb.Message{
		Type:    raftpb.MsgApp,
		From:    progress.TermHolder,
		Term:    progress.Term,
		LogTerm: progress.LogTerm,
		Index:   progress.LogIndex,
		Entries: progress.Entries,
		Commit:  progress.Commit,
	}
}

func (itp *OneToOneInterpreter) interpretVote(m *raftpb.Message) *raftpb.Message {
	var toRack, toFile string
	var rOk, fOk bool

	if toRack, rOk = itp.p2r[m.To]; !rOk {
		itp.lg.Warn("peer id is not bind to any rack", zap.Uint64("peer-id", m.To))
		return nil
	}

	if toFile, fOk = itp.p2f[m.From]; !fOk {
		itp.lg.Warn("peer id is not bind to any file path", zap.Uint64("peer-id", m.From))
		return nil
	}

	an := itp.an[toRack]

	if err := itp.writeToTargetFile(m, toRack, toFile, an); err != nil {
		return nil
	}

	zeroUpdate, vote := itp.getUpdatesFromOtherFiles(toRack, toFile, an)

	if zeroUpdate {
		return grantVote(m)
	}

	if vote != nil {
		if vote.Term > m.Term {
			// todo: check if legal
			return &raftpb.Message{
				Type:   raftpb.MsgVoteResp,
				To:     m.From,
				From:   vote.From,
				Term:   vote.Term,
				Reject: true,
			}
		} else if vote.Term == m.Term {
			// todo: check if legal
			return &raftpb.Message{
				Type:   raftpb.MsgVoteResp,
				To:     m.From,
				From:   m.To,
				Term:   m.Term,
				Reject: true,
			}
		}
	}

	progress, _ := an.Progress()

	if progress.NoProgress {
		return grantVote(m)
	}

	if progress.Term >= m.Term {
		// todo: check if legal
		// we find another living leader
		return &raftpb.Message{
			Type:    raftpb.MsgApp,
			To:      m.From,
			From:    progress.TermHolder,
			Term:    progress.Term,
			LogTerm: progress.LogTerm,
			Index:   progress.LogIndex,
			Entries: progress.Entries,
		}
	}

	if progress.LogIndex <= m.Index && progress.LogTerm <= m.Term {
		return grantVote(m)
	} else {
		// todo: check if legal
		// though our term is higher, our progress is not
		return &raftpb.Message{
			Type:    raftpb.MsgVoteResp,
			To:      m.From,
			From:    m.To,
			Term:    m.Term,
			Index:   progress.LogIndex,
			LogTerm: progress.LogTerm,
			Reject:  true,
		}
	}
}

func (itp *OneToOneInterpreter) interpretApp(m *raftpb.Message) *raftpb.Message {
	var toRack, toFile string
	var rOk, fOk bool

	if toRack, rOk = itp.p2r[m.To]; !rOk {
		itp.lg.Warn("peer id is not bind to any rack", zap.Uint64("peer-id", m.To))
		return nil
	}

	if toFile, fOk = itp.p2f[m.From]; !fOk {
		itp.lg.Warn("peer id is not bind to any file path", zap.Uint64("peer-id", m.From))
		return nil
	}

	an := itp.an[toRack]

	if err := itp.writeToTargetFile(m, toRack, toFile, an); err != nil {
		return nil
	}

	zeroUpdate, vote := itp.getUpdatesFromOtherFiles(toRack, toFile, an)

	if zeroUpdate {
		// if there is no other leaders, proceeding
		return handleAppendEntries(m, an)
	}

	if vote != nil {
		if vote.Term > m.Term {
			// todo: check if legal
			return &raftpb.Message{
				Type:   raftpb.MsgAppResp,
				To:     m.From,
				From:   vote.From,
				Term:   vote.Term,
				Reject: true,
			}
		}

		return handleAppendEntries(m, an)
	}

	progress, _ := an.Progress()

	if progress.NoProgress {
		return handleAppendEntries(m, an)
	}

	if progress.Term > m.Term {
		// todo: check if legal
		// we find another living leader
		return &raftpb.Message{
			Type:    raftpb.MsgApp,
			To:      m.From,
			From:    progress.TermHolder,
			Term:    progress.Term,
			LogTerm: progress.LogTerm,
			Index:   progress.LogIndex,
			Entries: progress.Entries,
		}
	}

	return handleAppendEntries(m, an)
}

func (itp *OneToOneInterpreter) writeToTargetFile(m *raftpb.Message, rack, targetFile string, an RackProgressAnalyzer) error {
	if err := itp.drp.Write(rack, targetFile, m); err != nil {
		itp.lg.Warn("error occurs when perform draft primitives",
			zap.String("op", "write"),
			zap.String("rack", rack),
			zap.String("file", targetFile),
			zap.Error(err),
		)

		return err
	}

	return nil
}

func (itp *OneToOneInterpreter) getUpdatesFromOtherFiles(rack, exceptFile string, an RackProgressAnalyzer) (bool, *raftpb.Message) {
	var count = len(itp.files) - 1
	var c = make(chan *Update, count)
	for _, file := range itp.files {
		if file != exceptFile {
			_ = itp.drp.AsyncGetUpdate(rack, file, c)
		}
	}

	var zeroDelta = true
	var vote *raftpb.Message = nil

	for update := range c {
		if update.Err != nil {
			itp.lg.Warn("error occurs when perform draft primitives",
				zap.String("op", "get-update"),
				zap.String("rack", rack),
				zap.String("file", update.SourceFile),
				zap.Error(update.Err),
			)
		} else if !update.ZeroDelta {
			zeroDelta = false
			if an.ConfirmLeadership(update.Term, itp.f2p[update.SourceFile]) {
				_ = an.TryOfferCollector(update.Term, itp.f2p[update.SourceFile], 0, update.Collected)
				if update.VoteMsg != nil {
					itp.lg.Info("detect another competitor", zap.String("source", update.SourceFile))
					if vote == nil || vote.Term < update.VoteMsg.Term {
						vote = update.VoteMsg
					}
				}
			}
		}

		count--
		if count == 0 {
			break
		}
	}

	return zeroDelta, vote
}

func grantVote(vote *raftpb.Message) *raftpb.Message {
	return &raftpb.Message{
		Type:   raftpb.MsgVoteResp,
		To:     vote.From,
		From:   vote.To,
		Term:   vote.Term,
		Reject: false,
	}
}

func handleAppendEntries(app *raftpb.Message, an RackProgressAnalyzer) *raftpb.Message {
	if app.Index < an.Commit() {
		return &raftpb.Message{
			Type:  raftpb.MsgAppResp,
			To:    app.From,
			From:  app.To,
			Term:  app.Term,
			Index: an.Commit(),
		}
	}

	ok, mLastIndex := an.TryOfferEntries(app.LogTerm, app.Index, app.Commit, app.Entries)
	if ok {
		return &raftpb.Message{
			Type:  raftpb.MsgAppResp,
			To:    app.From,
			From:  app.To,
			Term:  app.Term,
			Index: mLastIndex,
		}
	} else {
		return &raftpb.Message{
			Type:       raftpb.MsgAppResp,
			To:         app.From,
			From:       app.To,
			Term:       app.Term,
			Reject:     true,
			RejectHint: an.LastIndex(),
		}
	}
}

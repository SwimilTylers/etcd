package draft

import (
	"go.etcd.io/etcd/draft/collector"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

type Interpreter interface {
	//IsSupported checks if translation service is available to this Message.
	IsSupported(m *raftpb.Message) bool

	//Interpret takes in Raft Request and gives out Raft Response
	Interpret(m *raftpb.Message) []*raftpb.Message
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
	an  map[string]*MimicRaftKernelAnalyzer
}

func (itp *OneToOneInterpreter) IsSupported(m *raftpb.Message) bool {
	return m.Type == raftpb.MsgVote || m.Type == raftpb.MsgApp || m.Type == raftpb.MsgHeartbeat || m.Type == raftpb.MsgDRSync
}

func (itp *OneToOneInterpreter) Interpret(m *raftpb.Message) []*raftpb.Message {
	switch m.Type {
	case raftpb.MsgVote:
		if ok, r, f := itp.Locate(m); ok {
			return itp.interpretVote(r, f, m)
		}
	case raftpb.MsgApp:
		if ok, r, f := itp.Locate(m); ok {
			return itp.interpretApp(r, f, m)
		}
	case raftpb.MsgHeartbeat:
		if ok, r, f := itp.Locate(m); ok {
			return appRes2hbRes(itp.interpretApp(r, f, hb2app(m)))
		}
	case raftpb.MsgDRSync:
		return itp.drSync()
	}

	return nil
}

func (itp *OneToOneInterpreter) drSync() []*raftpb.Message {
	// during drSync, we do not involve voting
	an := itp.an[itp.syncRack]
	if noUpdates, _ := itp.getUpdatesFromOtherFiles(itp.syncRack, "", an); !noUpdates {
		an.AnalyzeAndRemoveOffers()
		if pg := an.Progress(); !pg.NoProgress {
			app := redirectAppendEntries(pg)
			// the prevLogTerm and prevLogIndex of an.beforeCompact
			// match the record in an.compacted, safe to compact
			an.Compact()
			return []*raftpb.Message{app}
		}
	}

	return nil
}

func (itp *OneToOneInterpreter) Locate(m *raftpb.Message) (accessibility bool, toRack, toFile string) {
	var rOk, fOk bool

	if toRack, rOk = itp.p2r[m.To]; !rOk {
		itp.lg.Warn("peer id is not bind to any rack", zap.Uint64("peer-id", m.To))
		return false, "", ""
	}

	if toFile, fOk = itp.p2f[m.From]; !fOk {
		itp.lg.Warn("peer id is not bind to any file path", zap.Uint64("peer-id", m.From))
		return false, "", ""
	}

	return true, toRack, toFile
}

func (itp *OneToOneInterpreter) interpretVote(toRack, toFile string, m *raftpb.Message) []*raftpb.Message {
	an := itp.an[toRack]

	if err := itp.writeToTargetFile(m, toRack, toFile, an); err != nil {
		return nil
	}

	// zeroUpdate, vote := itp.getUpdatesFromOtherFiles(toRack, toFile, an)

	/*
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

		progress := an.Progress()

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

	*/
	return nil
}

func (itp *OneToOneInterpreter) interpretApp(toRack, toFile string, m *raftpb.Message) []*raftpb.Message {
	an := itp.an[toRack]

	if err := itp.writeToTargetFile(m, toRack, toFile, an); err != nil {
		return nil
	}

	zeroUpdate, votes := itp.getUpdatesFromOtherFiles(toRack, toFile, an)
	an.AnalyzeAndRemoveOffers()

	// if there is no competitive challenger, move on
	if zeroUpdate || hasNoCompetitor(an) {
		resp := handleAppendEntries(m, an)
		return []*raftpb.Message{resp}
	}

	// otherwise, leadership is overthrown

	// scenario I: no pending votes
	if len(votes) == 0 {
		pg := an.Progress()
		switch {
		case pg.NoProgress:
			pg = an.UncheckedProgress()
			resp := newAppRespReject(m.From, m.To, pg.Term, m.Index, m.Index+uint64(len(m.Entries)))
			return []*raftpb.Message{resp}
		case pg.Term == m.Term:
			// our leadership
		}
	}

	/*
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
	*/
	return nil
}

func (itp *OneToOneInterpreter) writeToTargetFile(m *raftpb.Message, rack, targetFile string, an *MimicRaftKernelAnalyzer) error {
	if err := itp.drp.Write(rack, targetFile, m); err != nil {
		itp.lg.Warn("error occurs when perform draft primitives",
			zap.String("op", "write"),
			zap.String("rack", rack),
			zap.String("file", targetFile),
			zap.Error(err),
		)

		return err
	}

	if an != nil {
		switch m.Type {
		case raftpb.MsgVote:
			an.GetVoteAnalyzer().OfferLocalVote(m.Term, m.From, m.Index, m.LogTerm)
		case raftpb.MsgApp, raftpb.MsgHeartbeat:
			an.OfferLocalEntries(m.Term, itp.f2p[targetFile], m.Commit, m.LogTerm, m.Entries)
		}
	}

	return nil
}

//getUpdatesFromOtherFiles get updates from file (except exceptFile) on rack and submit them to analyzer.
// If there is no update, return false. If there exists some pending votes, return them.
func (itp *OneToOneInterpreter) getUpdatesFromOtherFiles(rack, exceptFile string, an *MimicRaftKernelAnalyzer) (bool, []*raftpb.Message) {
	var count = len(itp.files) - 1
	var c = make(chan *Update, count)
	for _, file := range itp.files {
		if file != exceptFile {
			_ = itp.drp.AsyncGetUpdate(rack, file, c)
		}
	}

	var zeroDelta = true
	var vote []*raftpb.Message

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
			itp.lg.Info("receive an update from other file",
				zap.String("rack", rack),
				zap.String("file", update.SourceFile),
				zap.Uint64("term", update.Term),
				zap.Uint64("committed", update.Commit),
				zap.Bool("has-append", !update.Collected.IsNotInitialized()),
				zap.Bool("has-vote", update.EverVote != nil),
			)

			if an != nil {
				an.OfferRemoteEntries(update.Term, itp.f2p[update.SourceFile], update.Commit, update.Collected)
			}

			if update.EverVote != nil {
				if an != nil {
					// if ever involve voting, offer it to analyzer
					ev := update.EverVote
					an.GetVoteAnalyzer().OfferRemoteVote(ev.Term, ev.From, ev.Index, ev.LogTerm, update.VotePend)
				}

				// only pending voting will be submit to the caller
				if update.VotePend {
					vote = append(vote, update.EverVote)
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

func hb2app(m *raftpb.Message) *raftpb.Message {
	return nil
}

func appRes2hbRes(m []*raftpb.Message) []*raftpb.Message {
	return nil
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

func handleAppendEntries(app *raftpb.Message, an *MimicRaftKernelAnalyzer) *raftpb.Message {
	// mimic the procedure of handleAppendEntries in raft kernel

	committed := an.Committed()

	if app.Index < committed {
		return newAppRespAccept(app.From, app.To, an.Term(), committed)
	}

	lastNewIndex := app.Index + uint64(len(app.Entries))

	switch an.GetSubLocator(true).MatchIndex(app.Index, app.LogTerm) {
	case collector.UNDERFLOW:
		panic("underflow occurs when making response")
	case collector.PREV, collector.WITHIN:
		return newAppRespAccept(app.From, app.To, an.Term(), lastNewIndex)
	default:
		return newAppRespReject(app.From, app.To, an.Term(), app.Index, lastNewIndex)
	}
}

func hasNoCompetitor(an *MimicRaftKernelAnalyzer) bool {
	return true
}

func redirectAppendEntries(pg *RackProgressDescriptor) *raftpb.Message {

	return nil
}

func newVoteRespAccept(voteSender, voteResponder, term, index uint64) *raftpb.Message {
	return nil
}

func newVoteRespReject(voteSender, voteResponder, term, index, rejectHint uint64) *raftpb.Message {
	return nil
}

func newAppRespAccept(appSender, appResponder, term, index uint64) *raftpb.Message {
	return &raftpb.Message{
		Type:  raftpb.MsgAppResp,
		To:    appSender,
		From:  appResponder,
		Term:  term,
		Index: index,
	}
}

func newAppRespReject(appSender, appResponder, term, index, rejectHint uint64) *raftpb.Message {
	return &raftpb.Message{
		Type:       raftpb.MsgAppResp,
		To:         appSender,
		From:       appResponder,
		Term:       term,
		Index:      index,
		Reject:     true,
		RejectHint: rejectHint,
	}
}

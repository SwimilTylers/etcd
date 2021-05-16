package draft

import (
	"go.etcd.io/etcd/draft/collector"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

type DrSyncType string

const (
	DrSyncPending DrSyncType = "DrSyncPending"
	DrSyncEntries DrSyncType = "DrSyncEntries"
	DrSyncAdvance DrSyncType = "DrSyncAdvance"
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
	return m.Type == raftpb.MsgVote || m.Type == raftpb.MsgPreVote ||
		m.Type == raftpb.MsgApp || m.Type == raftpb.MsgHeartbeat ||
		m.Type == raftpb.MsgDRSync
}

func (itp *OneToOneInterpreter) Interpret(m *raftpb.Message) *raftpb.Message {
	switch m.Type {
	case raftpb.MsgPreVote, raftpb.MsgVote:
		if ok, r, f := itp.Locate(m); ok {
			return itp.interpretVote(r, f, m)
		}
	case raftpb.MsgApp:
		if ok, r, f := itp.Locate(m); ok {
			return itp.interpretApp(r, f, m)
		}
	case raftpb.MsgDRSync:
		return itp.drSync(m)
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

func (itp *OneToOneInterpreter) drSync(m *raftpb.Message) *raftpb.Message {
	// during drSync, we do not involve voting
	an := itp.an[itp.syncRack]

	var u bool
	var votes []*raftpb.Message

	if u, votes = itp.getUpdatesFromOtherFiles(itp.syncRack, "", an); !u {
		// there is no update in racks, check out residual information in analyzer
		pg := an.Progress()

		switch {
		case !pg.NoProgress:
			an.Compact()
			return newDrSyncRespEntries(pg.Term, pg.TermHolder, pg.Commit, pg.LogIndex, pg.LogTerm, pg.Entries)
		case an.Term() > m.Term, an.Committed() > m.Commit:
			return newDrSyncRespAdvance(an.Term(), an.Committed())
		default:
			return nil
		}
	}

	an.AnalyzeAndRemoveOffers()
	an.TrySetTerm(m.Term)
	for _, v := range votes {
		an.TrySetTerm(v.Term)
	}

	pg := an.Progress()

	switch {
	case !pg.NoProgress:
		an.Compact()
		return newDrSyncRespEntries(pg.Term, pg.TermHolder, pg.Commit, pg.LogIndex, pg.LogTerm, pg.Entries)
	case an.Term() > m.Term, an.Committed() > m.Commit:
		return newDrSyncRespAdvance(an.Term(), an.Committed())
	default:
		return newDrSyncRespPending()
	}
}

func (itp *OneToOneInterpreter) interpretVote(toRack, toFile string, m *raftpb.Message) *raftpb.Message {
	an := itp.an[toRack]

	if an.Term() > m.Term {
		// a staled message, drop it
		itp.lg.Warn("receive a staled message from raft kernel",
			zap.Uint64("msg-term", m.Term),
			zap.Uint64("itp-term", an.Term()),
		)
		return nil
	}

	if err := itp.writeToTargetFile(m, toRack, toFile, nil); err != nil {
		itp.lg.Error("abort interpretation due to a write error", zap.Error(err))
		return nil
	}

	var u bool
	var votes []*raftpb.Message

	if u, votes = itp.getUpdatesFromOtherFiles(toRack, toFile, an); !u {
		// normal case
		return handleRequestVote(m, an.Term(), an.GetSubLocator(true))
	}

	an.AnalyzeAndRemoveOffers()
	an.TrySetTerm(m.Term)
	for _, v := range votes {
		an.TrySetTerm(v.Term)
	}

	var locator = an.GetSubLocator(!an.Progress().NoProgress)
	return handleRequestVote(m, an.Term(), locator)
}

func (itp *OneToOneInterpreter) interpretApp(toRack, toFile string, m *raftpb.Message) *raftpb.Message {
	an := itp.an[toRack]

	if an.Term() > m.Term {
		// a staled message, drop it
		itp.lg.Warn("receive a staled message from raft kernel",
			zap.Uint64("msg-term", m.Term),
			zap.Uint64("itp-term", an.Term()),
		)
		return nil
	}

	if err := itp.writeToTargetFile(m, toRack, toFile, an); err != nil {
		itp.lg.Error("abort interpretation due to a write error", zap.Error(err))
		return nil
	}

	var u bool
	var votes []*raftpb.Message

	if u, votes = itp.getUpdatesFromOtherFiles(toRack, toFile, an); !u {
		// normal case, merge local offer
		an.AnalyzeAndRemoveOffers()
		an.Compact()
		return handleAppendEntries(m, an.Committed(), an.Term(), an.GetSubLocator(true))
	}

	an.AnalyzeAndRemoveOffers()
	an.TrySetTerm(m.Term)
	for _, v := range votes {
		an.TrySetTerm(v.Term)
	}

	pg := an.Progress()

	switch {
	case pg.NoProgress, pg.Term == m.Term:
		// the progress is not overwritten
		//	1. the rack if lagging, permission for resent
		//  2. the rack receive staled entries from other processors, safe to proceed
		an.Compact()
		return handleAppendEntries(m, an.Committed(), an.Term(), an.GetSubLocator(true))
	default:
		// the progress is overwritten, locate our position
		var locator = an.GetSubLocator(true)
		if locator.LastIndex() < m.Index {
			locator = an.GetSubLocator(false)
		}

		return handleAppendEntries(m, an.Committed(), an.Term(), locator)
	}
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
		an.OfferLocalEntries(m.Term, itp.f2p[targetFile], m.Commit, m.LogTerm, m.Entries)
	}

	return nil
}

func (itp *OneToOneInterpreter) getUpdatesFromOtherFiles(rack, exceptFile string, an *MimicRaftKernelAnalyzer) (bool, []*raftpb.Message) {
	var count = len(itp.files) - 1
	var c = make(chan *Update, count)
	for _, file := range itp.files {
		if file != exceptFile {
			_ = itp.drp.AsyncGetUpdate(rack, file, c)
		}
	}

	var updated = false
	var vote []*raftpb.Message

	for i := 0; i < count; i++ {
		update := <-c

		if update.Err != nil {
			itp.lg.Warn("error occurs when perform draft primitives",
				zap.String("op", "get-update"),
				zap.String("rack", rack),
				zap.String("file", update.SourceFile),
				zap.Error(update.Err),
			)
		} else if !update.ZeroDelta {
			updated = true
			itp.lg.Info("receive an update from other file",
				zap.String("rack", rack),
				zap.String("file", update.SourceFile),
				zap.Bool("has-app", update.App != nil),
				zap.Bool("has-vote", update.Vote != nil),
			)

			if an != nil && update.App != nil {
				app := update.App
				an.OfferRemoteEntries(app.Term, itp.f2p[update.SourceFile], app.Commit, app.AE)
			}

			if update.Vote != nil {
				// only pending voting will be submit to the caller
				vote = append(vote, update.Vote)
			}
		}
	}

	return updated, vote
}

func handleAppendEntries(app *raftpb.Message, committed, term uint64, locator collector.Locator) *raftpb.Message {
	// mimic the procedure of handleAppendEntries in raft kernel

	if app.Index < committed {
		return newAppRespAccept(app.From, app.To, term, committed)
	}

	lastNewIndex := app.Index + uint64(len(app.Entries))

	switch locator.MatchIndex(app.Index, app.LogTerm) {
	case collector.UNDERFLOW:
		panic("underflow occurs when making response")
	case collector.PREV, collector.WITHIN:
		return newAppRespAccept(app.From, app.To, term, lastNewIndex)
	default:
		return newAppRespReject(app.From, app.To, term, app.Index, lastNewIndex)
	}
}

func handleRequestVote(vote *raftpb.Message, term uint64, locator collector.Locator) *raftpb.Message {
	preVote := vote.Type == raftpb.MsgPreVote

	// if the rack has the same term with our vote, reject our vote immediately in case of parallel vote-accept
	if vote.Term <= term {
		return newVoteRespReject(preVote, vote.From, vote.To, term)
	}

	if isUpdateTo(vote, locator) {
		return newVoteRespAccept(preVote, vote.From, vote.To, term)
	} else {
		return newVoteRespReject(preVote, vote.From, vote.To, term)
	}
}

func isUpdateTo(m *raftpb.Message, locator collector.Locator) bool {
	lastIndex := locator.LastIndex()
	_, lastTerm := locator.LocateIndex(lastIndex)

	if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= lastIndex) {
		return true
	} else {
		return false
	}
}

func hasProgress(m *raftpb.Message, committed uint64, pg *RackProgressDescriptor) bool {
	return m.Commit < committed || !pg.NoProgress
}

func newVoteRespAccept(preVote bool, voteSender, voteResponder, term uint64) *raftpb.Message {
	var rType = raftpb.MsgVoteResp
	if preVote {
		rType = raftpb.MsgPreVoteResp
	}

	return &raftpb.Message{
		Type: rType,
		To:   voteSender,
		From: voteResponder,
		Term: term,
	}
}

func newVoteRespReject(preVote bool, voteSender, voteResponder, term uint64) *raftpb.Message {
	var rType = raftpb.MsgVoteResp
	if preVote {
		rType = raftpb.MsgPreVoteResp
	}

	return &raftpb.Message{
		Type:   rType,
		To:     voteSender,
		From:   voteResponder,
		Term:   term,
		Reject: true,
	}
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

func newDrSyncRespPending() *raftpb.Message {
	return &raftpb.Message{
		Type:    raftpb.MsgDRSyncResp,
		Context: []byte(DrSyncPending),
	}
}

func newDrSyncRespEntries(term, tHolder, committed, prevLogIndex, prevLogTerm uint64, entries []raftpb.Entry) *raftpb.Message {
	return &raftpb.Message{
		Type:    raftpb.MsgDRSyncResp,
		Term:    term,
		From:    tHolder,
		Index:   prevLogIndex,
		LogTerm: prevLogTerm,
		Entries: entries,
		Commit:  committed,
		Context: []byte(DrSyncEntries),
	}
}

func newDrSyncRespAdvance(term, committed uint64) *raftpb.Message {
	return &raftpb.Message{
		Type:    raftpb.MsgDRSyncResp,
		Term:    term,
		Commit:  committed,
		Context: []byte(DrSyncAdvance),
	}
}

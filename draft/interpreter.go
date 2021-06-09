package draft

import (
	"go.etcd.io/etcd/draft/collector"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"sync"
)

type DrSyncType string

const (
	DrSyncPending DrSyncType = "DrSyncPending"
	DrSyncEntries DrSyncType = "DrSyncEntries"
	DrSyncAdvance DrSyncType = "DrSyncAdvance"
)

// Interpreter translate messages from raft.raft and generate response
type Interpreter interface {
	//IsSupported checks if translation service is available to this Message.
	IsSupported(m *raftpb.Message) bool

	//Interpret takes in Raft Request and gives out Raft Response
	Interpret(m *raftpb.Message) *raftpb.Message
}

type OneToOneInterpreterBuilder struct {
	self  uint64
	qSize int

	ids []uint64

	racks []string
	files []string

	p2r map[uint64]string
	p2f map[uint64]string
	f2p map[string]uint64
	r2a map[string]*MimicRaftKernelAnalyzer

	costumedDrp  PreservablePrimitives
	defaultDrp   *PrimitiveProvider
	mockAnalyzer *MimicRaftKernelAnalyzer
}

func NewOneToOneInterpreterBuilder(self uint64) *OneToOneInterpreterBuilder {
	return &OneToOneInterpreterBuilder{
		self:         self,
		p2r:          make(map[uint64]string),
		p2f:          make(map[uint64]string),
		f2p:          make(map[string]uint64),
		r2a:          make(map[string]*MimicRaftKernelAnalyzer),
		defaultDrp:   NewPrimitiveProvider(),
		mockAnalyzer: &MimicRaftKernelAnalyzer{},
	}
}

func (b *OneToOneInterpreterBuilder) Map(id uint64, rack, file string) *OneToOneInterpreterBuilder {
	_, rHas := b.r2a[rack]
	_, fHas := b.f2p[file]

	if rHas || fHas {
		panic("multiple mapping detected")
	}

	b.qSize++
	b.r2a[rack] = b.mockAnalyzer
	b.f2p[file] = id

	b.p2r[id] = rack
	b.p2f[id] = file

	b.ids = append(b.ids, id)
	b.racks = append(b.racks, rack)
	b.files = append(b.files, file)

	return b
}

func (b *OneToOneInterpreterBuilder) Bind(rack, file string, writer IMFWriter, reader IMFReader) *OneToOneInterpreterBuilder {
	if writer != nil {
		b.defaultDrp.GrantWrite(rack, file, writer)
	}

	if reader != nil {
		b.defaultDrp.GrantRead(rack, file, reader, collector.NewMultiFragmentsCollector())
	}

	return b
}

func (b *OneToOneInterpreterBuilder) UseCostumedPrimitive(drp *PrimitiveProvider) *OneToOneInterpreterBuilder {
	if drp != nil {
		panic("illegal argument")
	}

	b.costumedDrp = drp
	return b
}

func (b *OneToOneInterpreterBuilder) Build(lg *zap.Logger) *OneToOneInterpreter {
	if b.qSize == 0 {
		return nil
	}

	for _, r := range b.racks {
		b.r2a[r] = NewMimicRaftKernelAnalyzer(b.qSize)
	}

	var drp PreservablePrimitives = b.defaultDrp

	if b.costumedDrp != nil {
		drp = b.costumedDrp
	}

	return &OneToOneInterpreter{
		lg:       lg,
		racks:    b.racks,
		syncRack: b.p2r[b.self],
		p2r:      b.p2r,
		files:    b.files,
		p2f:      b.p2f,
		f2p:      b.f2p,
		drp:      drp,
		an:       b.r2a,
	}
}

func (b *OneToOneInterpreterBuilder) BuildParallel(lg *zap.Logger, cBufSize int) ParallelInterpreter {
	if b.qSize == 0 {
		return nil
	}

	itp := b.Build(lg)
	return &parallelInterpreter{
		lg:        lg,
		bufSize:   cBufSize,
		mu:        &sync.Mutex{},
		itp:       itp,
		reach:     b.ids,
		onRunning: false,
	}
}

// OneToOneInterpreter is an implementation of Interpreter. It maps an peer id to one rack.
// It is recommended to use OneToOneInterpreterBuilder for instantiating and populating.
type OneToOneInterpreter struct {
	lg *zap.Logger

	racks    []string
	syncRack string
	p2r      map[uint64]string

	files []string
	p2f   map[uint64]string
	f2p   map[string]uint64

	drp PreservablePrimitives
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
		if ok, r, f, an := itp.Locate(m); ok {
			return itp.interpretVote(r, f, an, m)
		}
	case raftpb.MsgApp:
		if ok, r, f, an := itp.Locate(m); ok {
			return itp.interpretApp(r, f, an, m)
		}
	case raftpb.MsgHeartbeat:
		if ok, r, f, an := itp.Locate(m); ok {
			itp.interpretHb(r, f, an, m)
		}
	case raftpb.MsgDRSync:
		if m.To == 0 {
			return itp.interpretDs(itp.syncRack, itp.an[itp.syncRack], m)
		} else if ok, r, _, an := itp.Locate(m); ok {
			return itp.interpretDs(r, an, m)
		}
	}

	return nil
}

func (itp *OneToOneInterpreter) Locate(m *raftpb.Message) (accessibility bool, toRack, toFile string, an *MimicRaftKernelAnalyzer) {
	var rOk, fOk bool

	if toRack, rOk = itp.p2r[m.To]; !rOk {
		itp.lg.Warn("peer id is not bind to any rack", zap.Uint64("peer-id", m.To))
		return false, "", "", nil
	}

	if toFile, fOk = itp.p2f[m.From]; !fOk {
		itp.lg.Warn("peer id is not bind to any file path", zap.Uint64("peer-id", m.From))
		return false, "", "", nil
	}

	return true, toRack, toFile, itp.an[toRack]
}

func (itp *OneToOneInterpreter) drSync(rack string, localTerm uint64, an *MimicRaftKernelAnalyzer) (ready, rollback bool, err error) {
	var u bool
	var votes []*raftpb.Message

	if !an.Analyzed() {
		panic("illegal state of analyzer")
	}

	if u, votes, err = itp.getUpdatesFromOtherFiles(rack, "", an); err != nil {
		return false, false, err
	}

	// nothing for analysis

	if !u {
		an.TrySetTerm(localTerm)
		return false, false, nil
	}

	// analyze updates

	if len(votes) == 0 {
		// log replication undergoing
		an.AnalyzeAndRemoveOffers(MatchLastFragment)
		an.TrySetTerm(localTerm)
		for _, v := range votes {
			an.TrySetTerm(v.Term)
		}
		return true, false, nil
	}

	// analyze votes

	if an.Analyzed() {
		// receive none AEUpdate from rack, leader does not appear yet
		an.TrySetTerm(localTerm)
		for _, v := range votes {
			an.TrySetTerm(v.Term)
		}
		return true, false, nil
	} else {
		// receive some AEUpdate from rack, be alert
		_, beforePg := an.UncheckedProgress()
		onVoting := false
		for _, v := range votes {
			if v.Term > beforePg.Term {
				onVoting = true
				break
			}
		}

		// election has come out a result, safe to log replication analysis
		if !onVoting {
			an.AnalyzeAndRemoveOffers(MatchLastFragment)
			an.TrySetTerm(localTerm)
			return true, false, nil
		}

		// election is undergoing, check potential progress conflict
		sba := NewSandboxForMimicRaftKernelAnalyzer(an)

		// get a snapshot of analyzer
		sba.PrepareSandbox()

		// analyzing based on the snapshot
		sba.GetSandbox().AnalyzeAndRemoveOffers(MatchLastFragment)
		sbxPg := sba.GetSandbox().Progress()

		if sbxPg.NoProgress || hasMatchedVote(sbxPg, votes) {
			// no interference, commit sandbox
			sba.CommitSandbox()
			an.TrySetTerm(localTerm)
			for _, v := range votes {
				an.TrySetTerm(v.Term)
			}
			return true, false, nil
		} else {
			// interfere the election, rollback
			itp.rollbackAnalyzer(rack, an, votes)
			return false, true, nil
		}
	}
}

func (itp *OneToOneInterpreter) interpretDs(rack string, an *MimicRaftKernelAnalyzer, m *raftpb.Message) *raftpb.Message {
	// during interpretDs, we do not involve voting
	var ready bool
	var rollback bool
	var err error

	if ready, rollback, err = itp.drSync(rack, m.Term, an); err != nil {
		itp.lg.Error("abort synchronization due to a read error", zap.Error(err))
		return nil
	}

	pg := an.Progress()

	switch {
	case !pg.NoProgress:
		an.Compact()
		return newDrSyncRespEntries(pg.Term, pg.TermHolder, pg.Commit, pg.LogIndex, pg.LogTerm, pg.Entries)
	case an.Term() > m.Term, an.Committed() > m.Commit:
		return newDrSyncRespAdvance(an.Term(), an.Committed())
	case ready:
		return newDrSyncRespPending(false)
	case rollback:
		return newDrSyncRespPending(true)
	default:
		return nil
	}
}

func (itp *OneToOneInterpreter) interpretVote(toRack, toFile string, an *MimicRaftKernelAnalyzer, m *raftpb.Message) *raftpb.Message {
	if an.Term() > m.Term {
		// a staled message, drop it
		itp.lg.Warn("receive a staled message from raft kernel",
			zap.Uint64("msg-term", m.Term),
			zap.Uint64("itp-term", an.Term()),
		)
		return nil
	}

	if err := itp.writeToTargetFile(m, toRack, toFile, nil); err != nil {
		itp.lg.Error("abort interpretation due to a write error",
			zap.String("msg", "vote"),
			zap.Error(err),
		)
		return nil
	}

	var u bool
	var votes []*raftpb.Message
	var err error

	if u, votes, err = itp.getUpdatesFromOtherFiles(toRack, toFile, an); err != nil {
		itp.lg.Error("abort interpretation due to a read error",
			zap.String("msg", "vote"),
			zap.Error(err),
		)
		return nil
	}

	if !u {
		// normal case
		resp := handleRequestVote(m, an.Term(), an.GetSubLocator(true))
		an.TrySetTerm(m.Term)
		return resp
	}

	an.AnalyzeAndRemoveOffers(MatchFirstFragment)
	for _, v := range votes {
		an.TrySetTerm(v.Term)
	}

	var locator = an.GetSubLocator(!an.Progress().NoProgress)
	resp := handleRequestVote(m, an.Term(), locator)
	an.TrySetTerm(m.Term)
	return resp
}

func (itp *OneToOneInterpreter) interpretApp(toRack, toFile string, an *MimicRaftKernelAnalyzer, m *raftpb.Message) *raftpb.Message {
	if an.Term() > m.Term {
		// a staled message, drop it
		itp.lg.Warn("receive a staled message from raft kernel",
			zap.Uint64("msg-term", m.Term),
			zap.Uint64("itp-term", an.Term()),
		)
		return nil
	}

	if err := itp.writeToTargetFile(m, toRack, toFile, an); err != nil {
		itp.lg.Error("abort interpretation due to a write error",
			zap.String("msg", "app"),
			zap.Error(err),
		)
		return nil
	}

	var u bool
	var votes []*raftpb.Message
	var err error

	if u, votes, err = itp.getUpdatesFromOtherFiles(toRack, toFile, an); err != nil {
		itp.lg.Error("abort interpretation due to a read error",
			zap.String("msg", "app"),
			zap.Error(err),
		)
		return nil
	}

	if !u {
		// normal case, merge local offer
		an.AnalyzeAndRemoveOffers(MatchFirstFragment)
		an.Compact()
		return handleAppendEntries(m, an.Committed(), an.Term(), an.GetSubLocator(true))
	}

	an.AnalyzeAndRemoveOffers(MatchFirstFragment)
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

func (itp *OneToOneInterpreter) interpretHb(toRack, toFile string, an *MimicRaftKernelAnalyzer, m *raftpb.Message) *raftpb.Message {
	if an.Term() > m.Term {
		// a staled message, drop it
		itp.lg.Warn("receive a staled message from raft kernel",
			zap.Uint64("msg-term", m.Term),
			zap.Uint64("itp-term", an.Term()),
		)
		return nil
	}

	if err := itp.writeToTargetFile(m, toRack, toFile, an); err != nil {
		itp.lg.Error("abort interpretation due to a write error",
			zap.String("msg", "heartbeat"),
			zap.Error(err),
		)
		return nil
	}

	var u bool
	var votes []*raftpb.Message
	var err error

	if u, votes, err = itp.getUpdatesFromOtherFiles(toRack, toFile, an); err != nil {
		itp.lg.Error("abort interpretation due to a read error",
			zap.String("msg", "heartbeat"),
			zap.Error(err),
		)
		return nil
	}

	if !u {
		// normal case, merge local offer
		an.AnalyzeAndRemoveOffers(MatchLastFragment)
		an.Compact()
		return handleHeartbeat(m, an.Term())
	}

	an.AnalyzeAndRemoveOffers(MatchLastFragment)
	an.TrySetTerm(m.Term)

	for _, v := range votes {
		an.TrySetTerm(v.Term)
	}

	return handleHeartbeat(m, an.Term())
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

func (itp *OneToOneInterpreter) getUpdatesFromOtherFiles(rack, exceptFile string, an *MimicRaftKernelAnalyzer) (bool, []*raftpb.Message, error) {
	var count = len(itp.files) - 1
	var c = make(chan *Update, count)
	for _, file := range itp.files {
		if file != exceptFile {
			_ = itp.drp.AsyncGetUpdate(rack, file, c)
		}
	}

	var updated = false
	var vote []*raftpb.Message
	var err error

	for i := 0; i < count; i++ {
		update := <-c

		if update.Err != nil {
			itp.lg.Warn("error occurs when perform draft primitives",
				zap.String("op", "get-update"),
				zap.String("rack", rack),
				zap.String("file", update.SourceFile),
				zap.Error(update.Err),
			)
			err = update.Err
			break
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

	return updated, vote, err
}

func (itp *OneToOneInterpreter) rollbackAnalyzer(rack string, an *MimicRaftKernelAnalyzer, votes []*raftpb.Message) {
	an.DropOffers()

	// here, an.Analyzed() == true

	if len(votes) > 0 {
		pVotes := make(map[string]*raftpb.Message, len(votes))
		for _, v := range votes {
			pVotes[itp.p2f[v.From]] = v
		}

		for _, file := range itp.files {
			_ = itp.drp.Preserve(rack, file, pVotes[file])
		}
	} else {
		for _, file := range itp.files {
			_ = itp.drp.Preserve(rack, file, nil)
		}
	}
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

func handleHeartbeat(hb *raftpb.Message, term uint64) *raftpb.Message {
	return newHbRespAccept(hb.From, hb.To, term, hb.Context)
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

func hasMatchedVote(pg *RackProgressDescriptor, votes []*raftpb.Message) bool {
	lastEnt := pg.Entries[len(pg.Entries)-1]
	lastLogTerm, lastLogIndex := lastEnt.Term, lastEnt.Index

	for _, v := range votes {
		if v.LogTerm == lastLogTerm && v.Index >= lastLogIndex {
			return true
		}
	}

	return false
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

func newHbRespAccept(hbSender, hbResponder, term uint64, ctx []byte) *raftpb.Message {
	return &raftpb.Message{
		Type:    raftpb.MsgHeartbeatResp,
		To:      hbSender,
		From:    hbResponder,
		Term:    term,
		Context: ctx,
	}
}

func newHbRespReject(hbSender, hbResponder, term, rejectHint uint64, ctx []byte) *raftpb.Message {
	return &raftpb.Message{
		Type:       raftpb.MsgHeartbeatResp,
		To:         hbSender,
		From:       hbResponder,
		Term:       term,
		Reject:     true,
		RejectHint: rejectHint,
		Context:    ctx,
	}
}

func newDrSyncRespPending(forRollback bool) *raftpb.Message {
	return &raftpb.Message{
		Type:    raftpb.MsgDRSyncResp,
		Reject:  forRollback,
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

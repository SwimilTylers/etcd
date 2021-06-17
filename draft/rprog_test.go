package draft

import (
	"fmt"
	"go.etcd.io/etcd/draft/collector"
	"go.etcd.io/etcd/raft/raftpb"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestMimicRaftKernelAnalyzer_AnalyzeAndRemoveOffersOneStep(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	round := 15

	for s := 0; s < round; s++ {
		var seed = time.Now().UnixNano()
		rnd := rand.New(rand.NewSource(seed))

		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("a panic occurs: seed=%v", seed)
				}
			}()
			oneStep := func(policy AnalysisPolicy, text string) {
				desc, oracle, an := runTestForOneStep(ids, rnd, policy)
				for i := 0; i < len(desc); i++ {
					if len(oracle[i].ent) == 0 {
						if !an[i].beforeCompact.IsEmpty() {
							t.Fatalf("one step scenario ([%s,%d]=\"%s\") inconsistent: seed=%v", text, i, desc[i], seed)
						}
					} else {
						if !oracle[i].EquivEntrySeq(an[i].beforeCompact) {
							t.Fatalf("one step scenario ([%s,%d]=\"%s\") inconsistent: seed=%v", text, i, desc[i], seed)
						}
					}
				}
			}

			oneStep(MatchFirstFragment, "match-first")
			oneStep(MatchLastFragment, "match-last")
		})
	}
}

func TestMimicRaftKernelAnalyzer_AnalyzeAndRemoveOffersMultiSteps(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	round := 15

	for s := 0; s < round; s++ {
		var seed = time.Now().UnixNano()
		rnd := rand.New(rand.NewSource(seed))

		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("a panic occurs: seed=%v", seed)
				}
			}()
			multiSteps := func(policy AnalysisPolicy, text string) {
				desc, oracle, an := runTestForMultiSteps(ids, rnd, policy)
				an.Compact()
				if !oracle.GetOracle("exact")(an.compacted) {
					if !oracle.GetOracle("inherit")(an.compacted) {
						t.Fatalf("multi steps scenario (%s, %s) inconsistent: seed=%v", text, desc, seed)
					} else {
						t.Logf("multi steps scenario (%s, %s) not exactly same, but follows an inheritent relation: seed=%v", text, desc, seed)
					}
				}
			}

			multiSteps(MatchFirstFragment, "match-first")
			multiSteps(MatchLastFragment, "match-last")
		})
	}
}

func TestMimicRaftKernelAnalyzer_AnalyzeAndRemoveOffersOneStepOnce(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	var seed int64 = 1623927697025628000
	rnd := rand.New(rand.NewSource(seed))

	oneStep := func(policy AnalysisPolicy, text string) {
		desc, oracle, an := runTestForOneStep(ids, rnd, policy)
		for i := 0; i < len(desc); i++ {
			if len(oracle[i].ent) == 0 {
				if !an[i].beforeCompact.IsEmpty() {
					t.Fatalf("one step scenario ([%s,%d]=\"%s\") inconsistent", text, i, desc[i])
				}
			} else {
				if !oracle[i].EquivEntrySeq(an[i].beforeCompact) {
					t.Fatalf("one step scenario ([%s,%d]=\"%s\") inconsistent", text, i, desc[i])
				}
			}
		}
	}

	oneStep(MatchFirstFragment, "match-first")
	oneStep(MatchLastFragment, "match-last")
}

func TestMimicRaftKernelAnalyzer_AnalyzeAndRemoveOffersMultiStepsOnce(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	var seed int64 = 1623927797794407000
	rnd := rand.New(rand.NewSource(seed))

	multiSteps := func(policy AnalysisPolicy, text string) {
		desc, oracle, an := runTestForMultiSteps(ids, rnd, policy)
		an.Compact()
		if !oracle.GetOracle("exact")(an.compacted) {
			if !oracle.GetOracle("inherit")(an.compacted) {
				t.Fatalf("multi steps scenario (%s, %s) inconsistent", text, desc)
			} else {
				t.Logf("multi steps scenario (%s, %s) not exactly same, but follows an inheritent relation", text, desc)
			}
		}
	}

	multiSteps(MatchFirstFragment, "match-first")
	multiSteps(MatchLastFragment, "match-last")
}

func runTestForOneStep(ids []uint64, rnd *rand.Rand, policy AnalysisPolicy) ([]string, []*entryComparator, []*MimicRaftKernelAnalyzer) {
	bf := newEntryBranchForkOperator()
	majorLen := 5 + rnd.Intn(15)
	bf.AnchorMajorBranch(majorLen, rnd)

	forkPoint := []int{0, 1 + rnd.Intn(majorLen-2), 1 + rnd.Intn(majorLen-2), majorLen - 1}
	sort.Ints(forkPoint)
	branch := [][]string{make([]string, len(forkPoint)), make([]string, len(forkPoint))}
	for i, p := range forkPoint {
		if p == 0 {
			branch[0][i] = bf.ForkMajorBranch(p, 1+rnd.Intn(9), rnd)
			branch[1][i] = bf.ForkMajorBranch(p, 1+rnd.Intn(9), rnd)
		} else {
			branch[0][i] = bf.ForkMajorBranch(p, rnd.Intn(10), rnd)
			branch[1][i] = bf.ForkMajorBranch(p, rnd.Intn(10), rnd)
		}
	}

	var desc []string
	var oracles []*entryComparator
	var analyzers []*MimicRaftKernelAnalyzer

	for i := 0; i < len(forkPoint); i++ {
		for j := i; j < len(forkPoint); j++ {
			tUint := func(ascending bool) {
				u0, u1 := getUpdatesForTwoBranches(ids, bf, rnd, ascending, branch[0][i], branch[1][j])

				an := newAlignedMimicRaftKernelAnalyzerForTest(bf.GetMajor())

				desc = append(desc, fmt.Sprintf("[[term=%v,fork=%d],[term=%v,fork=%d]]", u0.App.Term, forkPoint[i], u1.App.Term, forkPoint[j]))

				an.OfferRemoteEntries(u0.App.Term, ids[1], u0.App.Commit, u0.App.AE)
				an.OfferRemoteEntries(u1.App.Term, ids[2], u1.App.Commit, u1.App.AE)
				an.AnalyzeAndRemoveOffers(policy)

				if u0.App.AE.IsRefreshed() && u1.App.AE.IsRefreshed() {
					oracles = append(oracles, newEntryComparator(0, 0, nil))
				} else {
					oracles = append(oracles, newEntryComparator(
						bf.GetMinor(
							branchChosenOracle(
								policy,
								branch[0][i], u0.App.Term, forkPoint[i], u0.App.AE.IsRefreshed(),
								branch[1][j], u1.App.Term, forkPoint[j], u1.App.AE.IsRefreshed(),
							),
							true,
						),
					))
				}

				analyzers = append(analyzers, an)
			}

			tUint(true)
			tUint(false)
		}
	}
	return desc, oracles, analyzers
}

func runTestForMultiSteps(ids []uint64, rnd *rand.Rand, policy AnalysisPolicy) (string, testOracle, *MimicRaftKernelAnalyzer) {
	bb := newMockingBranchBuilder()
	branches := bb.AutoDeclare(5+rnd.Intn(10), rnd)
	for bb.PrimeSize() > 1 {
		bb.AutoJoin(rnd)
	}

	branches = deleteBranches(branches, bb.GetRootBranches()...)

	var root string
	var bf *entryBranchForkOperator
	for s, o := range bb.AutoBuild(rnd) {
		root = s
		bf = o
		break
	}

	if bf == nil {
		panic("get empty branch")
	}

	if strings.Compare(bb.StringFromBranch(root), gatherTraceFromBranches(bf, bb.GetNonDerivativeBranches()...)) != 0 {
		panic("branch inconsistent")
	}

	tMap := assignUniqueTermAmongBranches(getMaxTermAmongBranches(bf), bf, rnd)
	tMap[bf.GetMajorName()] = bf.GetMajorTerm()
	us, ss, gdi := getUpdatesForBranches(ids, ids[0], extIds(ids[1:], len(branches), rnd), bf, tMap, branches)

	an := newAlignedMimicRaftKernelAnalyzerForTest(bf.GetMajor())

	for i, u := range us {
		if u.App != nil {
			an.OfferRemoteEntries(u.App.Term, ss[i], u.App.Commit, u.App.AE)
		}
	}

	an.AnalyzeAndRemoveOffers(policy)

	var walker mockingBranchWalker
	switch policy {
	case MatchFirstFragment:
		walker = matchFirst
	case MatchLastFragment:
		walker = matchLast
	default:
		panic("no walker prepared")
	}

	oracle := bb.Walk(walker, tMap)[root]

	desc := fmt.Sprintf("branches=%s, tree=\n%spp=%v, oracle=[%s], an=[%s]\n",
		gdi.branchesOrderly(removeClusterPrefix),
		bb.StringFromBranch(root),
		gdi.beforeAfter(removeClusterPrefix),
		removeClusterPrefix(oracle),
		removeClusterPrefix(checkAnalyserResult(an, bf, branches)),
	)

	return desc, bf.Oracle(oracle), an
}

func branchChosenOracle(policy AnalysisPolicy, branch0 string, term0 uint64, forkPoint0 int, empty0 bool, branch1 string, term1 uint64, forkPoint1 int, empty1 bool) string {
	if !empty0 && !empty1 {
		switch policy {
		case MatchFirstFragment:
			if term0 < term1 {
				switch {
				case forkPoint0 == forkPoint1:
					return branch1
				case forkPoint0 < forkPoint1:
					return branch0
				case forkPoint0 > forkPoint1:
					return branch1
				}
			} else if term0 > term1 {
				switch {
				case forkPoint0 == forkPoint1:
					return branch0
				case forkPoint0 < forkPoint1:
					return branch0
				case forkPoint0 > forkPoint1:
					return branch1
				}
			} else {
				panic("illegal arguments")
			}
		case MatchLastFragment:
			if term0 < term1 {
				return branch1
			} else if term0 > term1 {
				return branch0
			} else {
				panic("illegal arguments")
			}
		default:
			panic("no oracle provided")
		}
	} else if !empty1 {
		return branch1
	} else if !empty0 {
		return branch0
	} else {
		panic("illegal arguments")
	}

	return ""
}

func getUpdatesForTwoBranches(ids []uint64, bf *entryBranchForkOperator, rnd *rand.Rand, ascending bool, branch0 string, branch1 string) (*Update, *Update) {
	_, terms := generateMonotonicTerms(bf, rnd, ascending, []string{branch0, branch1})
	us, _, _ := getUpdatesForBranches(ids, ids[0], []uint64{ids[1], ids[2]}, bf, terms, []string{branch0, branch1})
	return us[0], us[1]
}

func getUpdatesForBranches(ids []uint64, receiveBy uint64, sendBy []uint64, bf *entryBranchForkOperator, terms map[string]uint64, branches []string) ([]*Update, []uint64, *gubDebugInfo) {
	st := newMockingStorageWrapper()

	pp := NewPrimitiveProvider()
	to := receiveBy

	for _, sender := range ids {
		if sender != receiveBy {
			r, f := ft2rf(sender, to)
			pp.GrantRead(r, f, st.Receiver(sender, to), collector.NewMultiFragmentsCollector())
		}
	}

	sort.Slice(branches, func(i, j int) bool {
		return terms[branches[i]] < terms[branches[j]]
	})

	sendQ := make(map[uint64][]string)
	for i, sender := range sendBy {
		sendQ[sender] = append(sendQ[sender], branches[i])
	}

	for sender, q := range sendQ {
		sort.Slice(q, func(i, j int) bool {
			return terms[q[i]] < terms[q[j]]
		})
		ij := st.Sender(sender, to)
		for _, branch := range q {
			lt, li, ent := bf.GetMinor(branch, true)
			ij.JumpTo(terms[branch], lt, li).Append(0, lt, li, ent)
		}
	}

	var us []*Update
	var sId []uint64

	for _, sender := range ids {
		if sender != receiveBy {
			r, f := ft2rf(sender, to)
			us = append(us, pp.GetUpdate(r, f))
			sId = append(sId, sender)
		}
	}

	gdi := &gubDebugInfo{orderedBranches: branches}
	for _, sender := range ids {
		if sender != receiveBy {
			gdi.sendQueue = append(gdi.sendQueue, sendQ[sender])
		}
	}
	for _, u := range us {
		gdi.collected = append(gdi.collected, checkCollectorResult(u, bf, branches))
	}

	return us, sId, gdi
}

type gubDebugInfo struct {
	orderedBranches []string
	sendQueue       [][]string
	collected       [][]string
}

func (gdi *gubDebugInfo) branchesOrderly(renameFunc func(name string) string) string {
	sb := strings.Builder{}
	sb.WriteByte('[')
	for i, branch := range gdi.orderedBranches {
		if renameFunc != nil {
			branch = renameFunc(branch)
		}
		if i == 0 {
			sb.WriteString(branch)
		} else {
			sb.WriteString("," + branch)
		}
	}
	sb.WriteByte(']')
	return sb.String()
}

func (gdi *gubDebugInfo) beforeSend(renameFunc func(name string) string) []string {
	res := make([]string, len(gdi.sendQueue))
	for i := 0; i < len(res); i++ {
		sb := &strings.Builder{}
		sb.WriteByte('[')
		bs := gdi.sendQueue[i]
		for j, b := range bs {
			if renameFunc != nil {
				b = renameFunc(b)
			}
			if j == 0 {
				sb.WriteString(b)
			} else {
				sb.WriteString("," + b)
			}
		}
		sb.WriteByte(']')
		res[i] = sb.String()
	}
	return res
}

func (gdi *gubDebugInfo) afterCollect(renameFunc func(name string) string) []string {
	res := make([]string, len(gdi.collected))
	for i := 0; i < len(res); i++ {
		sb := &strings.Builder{}
		sb.WriteByte('[')
		bs := gdi.collected[i]
		for j, b := range bs {
			if renameFunc != nil {
				b = renameFunc(b)
			}
			if j == 0 {
				sb.WriteString(b)
			} else {
				sb.WriteString("," + b)
			}
		}
		sb.WriteByte(']')
		res[i] = sb.String()
	}
	return res
}

func (gdi *gubDebugInfo) beforeAfter(renameFunc func(name string) string) []string {
	before := gdi.beforeSend(renameFunc)
	after := gdi.afterCollect(renameFunc)

	res := make([]string, len(before))
	for i := 0; i < len(before); i++ {
		res[i] = before[i] + "=>" + after[i]
	}

	return res
}

func checkCollectorResult(u *Update, bf *entryBranchForkOperator, branches []string) []string {
	if u.App == nil {
		return nil
	}

	c := u.App.AE
	if c.IsRefreshed() {
		return nil
	}

	_, fs := c.FetchAllFragments()
	var ub []string
	for _, f := range fs {
		last := f.Fragment[len(f.Fragment)-1]
		for _, branch := range branches {
			_, _, ent := bf.GetMinor(branch, true)
			if reflect.DeepEqual(last, ent[len(ent)-1]) {
				ub = append(ub, branch)
				break
			}
		}
	}

	if len(fs) != len(ub) {
		panic("cannot find corresponding branch")
	}

	return ub
}

func checkAnalyserResult(an *MimicRaftKernelAnalyzer, bf *entryBranchForkOperator, branches []string) string {
	an.Compact()

	if an.compacted.IsEmpty() {
		return bf.GetMajorName()
	}

	lastIndex := an.compacted.LastIndex()
	_, lastTerm := an.compacted.LocateIndex(lastIndex)

	var res = bf.GetMajorName()

	for _, branch := range branches {
		_, _, ent := bf.GetMinor(branch, true)
		last := ent[len(ent)-1]
		if last.Index == lastIndex && last.Term == lastTerm {
			res = branch
			break
		}
	}

	return res
}

var removeClusterPrefix = func(name string) string {
	return strings.Split(name, "-")[1]
}

func extIds(ids []uint64, size int, rnd *rand.Rand) []uint64 {
	res := make([]uint64, size)
	length := len(ids)
	for i := 0; i < size; i++ {
		res[i] = ids[rnd.Intn(length)]
	}
	return res
}

func generateMonotonicTerms(bf *entryBranchForkOperator, rnd *rand.Rand, ascending bool, branches []string) ([]uint64, map[string]uint64) {
	terms := make([]uint64, len(branches))
	var maxTerm uint64 = 0

	for i, b := range branches {
		terms[i] = bf.GetMinorTerm(b)
		if terms[i] > maxTerm {
			maxTerm = terms[i]
		}
	}

	delta := make([]uint64, len(terms))
	deltaMap := make(map[string]uint64, len(branches))
	term := maxTerm

	if ascending {
		for i := 0; i < len(terms); i++ {
			term += rnd.Uint64()&0xff + 1
			delta[i] = term
			deltaMap[branches[i]] = term
		}
	} else {
		for i := len(terms) - 1; i >= 0; i-- {
			term += rnd.Uint64()&0xff + 1
			delta[i] = term
			deltaMap[branches[i]] = term
		}
	}

	return delta, deltaMap
}

func newAlignedMimicRaftKernelAnalyzerForTest(logTerm, logIndex uint64, entries []raftpb.Entry) *MimicRaftKernelAnalyzer {
	an := NewMimicRaftKernelAnalyzer(10)
	// rebasing compacted
	an.compacted.Refresh()
	an.compacted.AddEntriesToBrief(entries, logTerm, logIndex)
	return an
}

var (
	matchFirst mockingBranchWalker = func(from *mockingBranchDescriptor, terms map[string]uint64) *mockingBranchDescriptor {
		base := terms[from.name]
		var res *mockingBranchDescriptor
		for i := 0; i <= from.extEntLen; i++ {
			for _, d := range from.derivedTo[i] {
				if terms[d.name] > base {
					base = terms[d.name]
					if d.extEntLen > 0 {
						res = d
					}
				}
			}
			if res != nil {
				break
			}
		}
		return res
	}

	matchLast mockingBranchWalker = func(from *mockingBranchDescriptor, terms map[string]uint64) *mockingBranchDescriptor {
		base := terms[from.name]
		var res *mockingBranchDescriptor
		for _, ds := range from.derivedTo {
			for _, d := range ds {
				if terms[d.name] > base {
					base = terms[d.name]
					if d.extEntLen > 0 {
						res = d
					}
				}
			}
		}
		return res
	}
)

package draft

import (
	"fmt"
	"go.etcd.io/etcd/draft/collector"
	"go.etcd.io/etcd/raft/raftpb"
	"math/rand"
	"sort"
	"testing"
)

func TestMimicRaftKernelAnalyzer_AnalyzeAndRemoveOffersOneStep(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	round := 15

	for r := 0; r < round; r++ {
		var seed = rand.Int63()
		rnd := rand.New(rand.NewSource(seed))

		t.Run("", func(t *testing.T) {
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

func TestMimicRaftKernelAnalyzer_AnalyzeAndRemoveOffersOneStepOnce(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	var seed int64 = 5577006791947779410
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

func runTestForOneStep(ids []uint64, rnd *rand.Rand, policy AnalysisPolicy) ([]string, []*entryComparator, []*MimicRaftKernelAnalyzer) {
	bf := newEntryBranchForkHelper()
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

				an := newTestMimicRaftKernelAnalyzer(bf.GetMajor())

				desc = append(desc, fmt.Sprintf("[[term=%v,fork=%d],[term=%v,fork=%d]]", u0.App.Term, forkPoint[i], u1.App.Term, forkPoint[j]))

				an.OfferRemoteEntries(u0.App.Term, ids[1], u0.App.Commit, u0.App.AE)
				an.OfferRemoteEntries(u1.App.Term, ids[2], u1.App.Commit, u1.App.AE)
				an.AnalyzeAndRemoveOffers(policy)

				if u0.App.AE.IsNotInitialized() && u1.App.AE.IsNotInitialized() {
					oracles = append(oracles, newEntryComparator(0, 0, nil))
				} else {
					oracles = append(oracles, newEntryComparator(
						bf.GetMinor(
							branchChosenOracle(
								policy,
								branch[0][i], u0.App.Term, forkPoint[i], u0.App.AE.IsNotInitialized(),
								branch[1][j], u1.App.Term, forkPoint[j], u1.App.AE.IsNotInitialized(),
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

func getUpdatesForTwoBranches(ids []uint64, bf *entryBranchForkHelper, rnd *rand.Rand, ascending bool, branch0 string, branch1 string) (*Update, *Update) {
	us := getUpdatesForBranches(ids, bf, rnd, ascending, branch0, branch1)
	return us[0], us[1]
}

func getUpdatesForBranches(ids []uint64, bf *entryBranchForkHelper, rnd *rand.Rand, ascending bool, branch ...string) []*Update {
	st := newMockingStorageWrapper()

	pp := NewPrimitiveProvider()
	to := ids[0]

	terms := make([]uint64, len(branch))
	var maxTerm uint64 = 0

	for i, b := range branch {
		terms[i] = bf.GetMinorTerm(b)
		if terms[i] > maxTerm {
			maxTerm = terms[i]
		}
	}

	delta := make([]uint64, len(terms))
	term := maxTerm

	if ascending {
		for i := 0; i < len(terms); i++ {
			term += rnd.Uint64()&0xff + 1
			delta[i] = term
		}
	} else {
		for i := len(terms) - 1; i >= 0; i-- {
			term += rnd.Uint64()&0xff + 1
			delta[i] = term
		}
	}

	f := func(branch string, rnd *rand.Rand, from, delta uint64) *Update {
		lt, li, ent := bf.GetMinor(branch, true)
		st.Sender(from, to).Vote(delta).Append(0, lt, li, ent)
		r, f := ft2rf(from, to)
		pp.GrantRead(r, f, st.Receiver(from, to), collector.NewMultiFragmentsCollector())
		return pp.GetUpdate(r, f)
	}

	us := make([]*Update, len(branch))

	for i := 0; i < len(us); i++ {
		us[i] = f(branch[i], rnd, ids[i+1], delta[i])
	}

	return us
}

func newTestMimicRaftKernelAnalyzer(logTerm, logIndex uint64, entries []raftpb.Entry) *MimicRaftKernelAnalyzer {
	an := NewMimicRaftKernelAnalyzer(10)
	// rebasing compacted
	an.compacted.Refresh()
	an.compacted.AddEntriesToBrief(entries, logTerm, logIndex)
	return an
}

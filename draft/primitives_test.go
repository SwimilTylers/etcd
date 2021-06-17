package draft

import (
	"fmt"
	"go.etcd.io/etcd/draft/collector"
	"go.etcd.io/etcd/raft/raftpb"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestEmptyUpdate(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	pps, _, _ := preparation(newMockingIMFStorage(), ids, false, false)
	for _, host := range ids {
		pp := pps[host]
		for _, to := range ids {
			for _, from := range ids {
				if from != host {
					r, f := ft2rf(from, to)
					if !reflect.DeepEqual(pp.GetUpdate(r, f), noUpdate(f)) {
						t.Fatalf("host=%v, rack='%s', file='%s': should get no update", host, r, f)
					}
				}
			}
		}
	}
}

func TestVoteUpdate(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	pps, mis, mrs := preparation(newMockingIMFStorage(), ids, false, true)

	for _, from := range ids {
		for _, to := range ids {
			if from != to {
				token := rf2t(ft2rf(from, to))
				mis[token].Vote(1)
				vote := mrs[token].GetTop()
				pp := pps[to]
				for _, other := range ids {
					if other != to {
						r, f := ft2rf(other, to)
						if other == from {
							if !reflect.DeepEqual(pp.GetUpdate(r, f), newUpdate(f, nil, vote)) {
								t.Fatalf("vote=%v, rack=%s, file=%s: should receive vote", from, r, f)
							}
						} else {
							if !reflect.DeepEqual(pp.GetUpdate(r, f), noUpdate(f)) {
								t.Fatalf("vote=%v, rack='%s', file='%s': should get no update", from, r, f)
							}
						}
					}
				}
			}
		}
	}
}

func TestAppendUpdate(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	pps, mis, mrs := preparation(newMockingIMFStorage(), ids, false, true)

	for _, from := range ids {
		for _, to := range ids {
			if from != to {
				token := rf2t(ft2rf(from, to))
				mis[token].AutoVote().AutoAppend()
				app := mrs[token].GetTop()
				pp := pps[to]
				for _, other := range ids {
					if other != to {
						r, f := ft2rf(other, to)
						if other == from {
							u := pp.GetUpdate(r, f)

							if u.Vote != nil {
								t.Fatalf("append=%v, rack=%s, file=%s: should receive no vote", from, r, f)
							}

							ok, fs := u.App.AE.FetchAllFragments()
							ent, lt, li := fs[0].Fragment, fs[0].LogTerm, fs[0].LogIndex
							if !ok || lt != app.LogTerm || li != app.Index || !reflect.DeepEqual(ent, app.Entries) {
								oks := fmt.Sprintf("[%v/%v]", ok, true)
								ents := fmt.Sprintf("ent=[%+v/%+v]", ent, app.Entries)
								lts := fmt.Sprintf("LogTerm=[%v/%v]", lt, app.LogTerm)
								lis := fmt.Sprintf("LogIndex=[%v/%v]", li, app.Index)
								t.Fatalf("append=%v, rack=%s, file=%s: collector borkened \n==>%s\t%s\t%s\t%s", from, r, f, oks, ents, lts, lis)
							}

							if !reflect.DeepEqual(u, newUpdate(f, &AEUpdate{app.Term, app.Commit, pp.collector[token].efc}, nil)) {
								t.Fatalf("append=%v, rack=%s, file=%s: should receive appropriate append", from, r, f)
							}
						} else {
							if !reflect.DeepEqual(pp.GetUpdate(r, f), noUpdate(f)) {
								t.Fatalf("vote=%v, rack='%s', file='%s': should get no update", from, r, f)
							}
						}
					}
				}
			}
		}
	}
}

func TestVoteWrite(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	pps, mis, mrs := preparation(newMockingIMFStorage(), ids, true, true)

	for _, host := range ids {
		pp := pps[host]

		votes := make(map[uint64]*raftpb.Message)

		for _, to := range ids {
			r, f := ft2rf(host, to)
			token := rf2t(r, f)
			mis[token].AutoVote()
			vote := mrs[token].GetTop()

			pp.Write(r, f, vote)
			votes[to] = vote
		}

		for _, other := range ids {
			ppo := pps[other]
			for _, to := range ids {
				for _, from := range ids {
					if from == other {
						continue
					}

					r, f := ft2rf(from, to)
					if from == host {
						if !reflect.DeepEqual(votes[to], ppo.GetUpdate(r, f).Vote) {
							t.Fatalf("prespective=%v, vote=%v, rack=%s, file=%s: should receive vote", other, from, r, f)
						}
					} else {
						if !ppo.GetUpdate(r, f).ZeroDelta {
							t.Fatalf("prespective=%v, vote=%v, rack=%s, file=%s: should no receive vote", other, from, r, f)
						}
					}
				}
			}
		}
	}
}

func TestAppendWrite(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	pps, mis, mrs := preparation(newMockingIMFStorage(), ids, true, true)

	for _, host := range ids {
		pp := pps[host]

		appends := make(map[uint64]*raftpb.Message)

		for _, to := range ids {
			r, f := ft2rf(host, to)
			token := rf2t(r, f)
			mis[token].AutoVote().AutoAppend()
			a := mrs[token].GetTop()

			pp.Write(r, f, a)
			appends[to] = a
		}

		for _, other := range ids {
			ppo := pps[other]
			for _, to := range ids {
				for _, from := range ids {
					if from == other {
						continue
					}

					r, f := ft2rf(from, to)
					if from == host {
						u := ppo.GetUpdate(r, f)

						_, fs := u.App.AE.FetchAllFragments()
						if !reflect.DeepEqual(appends[to].Entries, fs[0].Fragment) {
							t.Fatalf("prespective=%v, vote=%v, rack=%s, file=%s: should receive vote", other, from, r, f)
						}

						if appends[to].LogTerm != fs[0].LogTerm {
							//
						}

						if appends[to].Index != fs[0].LogIndex {
							//
						}

					} else {
						if !ppo.GetUpdate(r, f).ZeroDelta {
							t.Fatalf("prespective=%v, vote=%v, rack=%s, file=%s: should no receive vote", other, from, r, f)
						}
					}
				}
			}
		}
	}
}

func TestUpdateCollectedEntries(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	round := 15

	for i := 0; i < round; i++ {
		var seed = time.Now().UnixNano()
		rnd := rand.New(rand.NewSource(seed))
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("a panic occurs: seed=%v", seed)
				}
			}()
			mes, u, _ := runTestUpdateCollectedEntries(ids, rnd)
			if !mes.EquivEntrySeq(u.App.AE) {
				t.Errorf("entry is incorrectly collected: seed=%v", seed)
			}
			mes0, mes1, uu, _ := runTestUpdateCollectedEntriesOverride(ids, rnd)
			if mes0.EquivEntrySeq(uu.App.AE) || !mes1.EquivEntrySeq(uu.App.AE) {
				t.Errorf("joint entry is incorrectly collected: seed=%v", seed)
			}
		})
	}
}

func TestUpdateCollectedEntriesOnce(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	var seed int64 = 1623917564788772000
	rnd := rand.New(rand.NewSource(seed))

	mes, u, actions := runTestUpdateCollectedEntries(ids, rnd)

	if !mes.EquivEntrySeq(u.App.AE) {
		t.Errorf("entry is incorrectly collected: len=%v, actions=%v", mes.entLen, actions)
	}

	mes0, mes1, uu, desc := runTestUpdateCollectedEntriesOverride(ids, rnd)
	if mes0.EquivEntrySeq(uu.App.AE) || !mes1.EquivEntrySeq(uu.App.AE) {
		t.Errorf("entry is incorrectly collected: len0=%v, len1=%v, u=%s, actions=\n%v", mes0.entLen, mes1.entLen, desc[1], desc[0])
	}
}

func runTestUpdateCollectedEntries(ids []uint64, rnd *rand.Rand) (*mockingEntrySplitter, *Update, []string) {
	pps, mis, mrs := preparation(newMockingIMFStorage(), ids, true, true)

	plt, pli, entries := generateEntryDesc(5+rnd.Intn(15), rnd)

	leader := uint64(0)
	follower := uint64(1)

	lpp := pps[leader]
	r, f := ft2rf(leader, follower)
	token := rf2t(r, f)

	mes, actions := appendRandomWalk(plt, pli, entries, mis[token].AutoVote(), rnd)
	for _, m := range mrs[token].GetAll() {
		_ = lpp.Write(r, f, m)
	}

	u := pps[follower].GetUpdate(r, f)

	return mes, u, actions
}

func runTestUpdateCollectedEntriesOverride(ids []uint64, rnd *rand.Rand) (*mockingEntrySplitter, *mockingEntrySplitter, *Update, []string) {
	pps, mis, mrs := preparation(newMockingIMFStorage(), ids, true, true)

	plt, pli, entries0 := generateEntryDesc(5+rnd.Intn(15), rnd)
	eLen := len(entries0)
	_, _, entries1 := extendEntryDesc(1+rnd.Intn(14), rnd, plt, pli, entries0[:rnd.Intn(eLen+1)])

	leader := uint64(0)
	follower := uint64(1)

	lpp := pps[leader]
	r, f := ft2rf(leader, follower)
	token := rf2t(r, f)

	mes0, _ := appendRandomWalk(plt, pli, entries0, mis[token].AutoVote(), rnd)
	mes1, _ := appendRandomWalk(plt, pli, entries1, mis[token].AutoVote(), rnd)

	var desc = []string{messageToStrings(mrs[token].buf, entries0, entries1)}

	for _, m := range mrs[token].GetAll() {
		lpp.Write(r, f, m)
	}

	u := pps[follower].GetUpdate(r, f)

	var uInfo = "no update"

	if u.App != nil && !u.App.AE.IsRefreshed() {
		if _, fs := u.App.AE.FetchAllFragments(); len(fs) == 1 {
			ent := fs[0].Fragment
			if ent[0].Term == entries0[0] {
				uInfo = "[[all] []]"
			} else if ent[0].Term == entries1[0] {
				uInfo = "[[] [all]]"
			} else {
				uInfo = "[[some] [some]]"
			}
		} else {
			uInfo = "multiple fragments"
		}
	}

	desc = append(desc, uInfo)

	return mes0, mes1, u, desc
}

func appendRandomWalk(logTerm, logIndex uint64, entries []uint64, mi *mockingIMFInjector, rnd *rand.Rand) (*mockingEntrySplitter, []string) {
	mes := newMockingEntrySplitter(generateEntries(logTerm, logIndex, entries))
	var actionS []string

	length := len(entries)

	action := [][]func(){
		{
			// normal appending
			func() { mi.Append(mes.NextOneStep()); actionS = append(actionS, "p++") },
			func() {
				step := 1 + rnd.Intn(length)
				mi.Append(mes.Next(step))
				actionS = append(actionS, "p+="+strconv.Itoa(step))
			},
			func() { mi.Append(mes.NextAll()); actionS = append(actionS, "p=all") },
			func() { mi.Append(mes.NextTrivial()); actionS = append(actionS, "p+=0") },
		},
		{
			// resend entries
			func() {
				step := mes.MoveBackwards(1 + rnd.Intn(length))
				actionS = append(actionS, "i-="+strconv.Itoa(step))
			},
			func() { mes.MoveBackToZero(); actionS = append(actionS, "i=0") },
		},
		{
			// commit forwarding
			func() {
				step := mes.CommitForwards(1 + rnd.Intn(length))
				actionS = append(actionS, "c+="+strconv.Itoa(step))
			},
			func() { mes.CommitMost(); actionS = append(actionS, "c=all") },
		},
		{
			// lagged appending
			func() {
				pStep := 1 + rnd.Intn(length)
				mes.SetDispatchService(false)
				mes.Next(pStep)
				mes.SetDispatchService(true)
				iStep := mes.MoveBackwards(1 + rnd.Intn(length))
				mi.Append(mes.NextTrivial())
				actionS = append(actionS, "(p+="+strconv.Itoa(pStep)+",i-="+strconv.Itoa(iStep)+")")
			},
			func() {
				mes.SetDispatchService(false)
				mes.NextAll()
				mes.SetDispatchService(true)
				step := mes.MoveBackwards(1 + rnd.Intn(length))
				mi.Append(mes.NextTrivial())
				actionS = append(actionS, "(p=all,i-="+strconv.Itoa(step)+")")
			},
		},
	}

	for !mes.IsCommitAll() || !mes.IsDispatchAll() {
		n, m := rnd.Int(), rnd.Int()

		n %= len(action)
		m %= len(action[n])

		action[n][m]()
	}

	return mes, actionS
}

func preparation(storage *mockingIMFStorage, ids []uint64, dropInject bool, usingInjectRec bool) (map[uint64]*PrimitiveProvider, map[string]*mockingIMFInjector, map[string]*mockingIMFInjectorRec) {
	pps := make(map[uint64]*PrimitiveProvider)
	mis := make(map[string]*mockingIMFInjector)
	mrs := make(map[string]*mockingIMFInjectorRec)

	for _, host := range ids {
		pp := NewPrimitiveProvider()
		for _, to := range ids {
			for _, from := range ids {
				r, f := ft2rf(from, to)
				if from == host {
					pp.GrantWrite(r, f, storage.OfferWriteGrant(rf2t(r, f)))
				} else {
					pp.GrantRead(r, f, storage.OfferReadGrant(rf2t(r, f)), collector.NewMultiFragmentsCollector())
				}
			}
		}
		pps[host] = pp
	}

	for _, from := range ids {
		for _, to := range ids {
			r, f := ft2rf(from, to)
			var writer IMFWriter

			if dropInject {
				writer = storage.OfferRedirectedWriteGrant(rf2t(r, f), func(message *raftpb.Message) error { return nil })
			} else {
				writer = storage.OfferWriteGrant(rf2t(r, f))
			}

			if usingInjectRec {
				his := &mockingIMFInjectorRec{}
				mrs[rf2t(r, f)] = his

				mis[rf2t(r, f)] = newMockingMemorableIMFInjector(his).UseMemorable(writer).InitAs(0, from, to, 0, 0)
			} else {
				mis[rf2t(r, f)] = newMockingIMFInjector().Use(writer).InitAs(0, from, to, 0, 0)
			}

		}
	}

	return pps, mis, mrs
}

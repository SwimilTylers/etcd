package draft

import (
	"fmt"
	"go.etcd.io/etcd/raft/raftpb"
	"reflect"
	"testing"
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
							if !reflect.DeepEqual(pp.GetUpdate(r, f), newUpdate(f, 1, pp.collector[token], 0, vote)) {
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

							if u.VoteMsg != nil {
								t.Fatalf("append=%v, rack=%s, file=%s: should receive no vote", from, r, f)
							}

							ok, ent, lt, li := u.Collected.FetchAllEntries()
							if !ok || lt != app.LogTerm || li != app.Index || !reflect.DeepEqual(ent, app.Entries) {
								oks := fmt.Sprintf("[%v/%v]", ok, true)
								ents := fmt.Sprintf("ent=[%+v/%+v]", ent, app.Entries)
								lts := fmt.Sprintf("logTerm=[%v/%v]", lt, app.LogTerm)
								lis := fmt.Sprintf("logIndex=[%v/%v]", li, app.Index)
								t.Fatalf("append=%v, rack=%s, file=%s: collector borkened \n==>%s\t%s\t%s\t%s", from, r, f, oks, ents, lts, lis)
							}

							if !reflect.DeepEqual(u, newUpdate(f, app.Term, pp.collector[token], app.Commit, nil)) {
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
						if !reflect.DeepEqual(votes[to], ppo.GetUpdate(r, f).VoteMsg) {
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

						_, ent, lt, li := u.Collected.FetchAllEntries()
						if !reflect.DeepEqual(appends[to].Entries, ent) {
							t.Fatalf("prespective=%v, vote=%v, rack=%s, file=%s: should receive vote", other, from, r, f)
						}

						if appends[to].LogTerm != lt {
							//
						}

						if appends[to].Index != li {
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
	pps, mis, mrs := preparation(newMockingIMFStorage(), ids, true, true)

	plt := uint64(3)
	pli := uint64(4)
	entries := []uint64{3, 3, 4, 4, 5, 7, 7}

	mes := newMockingEntrySplitter(generateEntries(plt, pli, entries))

	leader := uint64(0)
	follower := uint64(1)

	lpp := pps[leader]
	r, f := ft2rf(leader, follower)
	token := rf2t(r, f)

	mi := mis[token].AutoVote()
	for i := 0; i < len(entries); i++ {
		mi.Append(mes.NextOneStep())
	}

	for _, m := range mrs[token].GetAll() {
		lpp.Write(r, f, m)
	}

	u := pps[follower].GetUpdate(r, f)
	_, ent, lt, li := u.Collected.FetchAllEntries()

	if plt != lt || pli != li || !reflect.DeepEqual(entries, ent) {
		t.Fatalf("entry is incorrectly collected")
	}

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
					pp.GrantRead(r, f, storage.OfferReadGrant(rf2t(r, f)), NewEntryFragmentCollector(true))
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

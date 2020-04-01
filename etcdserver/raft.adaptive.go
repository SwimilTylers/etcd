package etcdserver

import (
	"fmt"
	"go.etcd.io/etcd/adaptive"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"time"
)

const (
	// NORMAL MODE means that current peer network works fine.
	//
	// Therefore, AdaNode should prefer performance and reduce
	// the frequency of persistent operations.
	NORMAL uint8 = iota

	// SHELTERING MODE means that current peer network is at stake.
	//
	// Therefore, AdaNode should prefer reliability and insists to
	// persist.
	SHELTERING
)

type AdaNode struct {
	raftNode

	peers []uint64
	self  uint64

	// PeerMonitor perceives the connectivity of peers and decides
	// whether to switch between fast-but-not-reliable mode and
	// slow-but-reliable mode.
	//
	// To be more specific:
	//   1. PeerMonitor perceives connectivity from the result of heartbeat
	//   2. When SaucrMonitor.IsCritical is true, running in slow mode
	//   3. When SoftState changes, PeerMonitor has to reset
	PeerMonitor adaptive.Perceptible

	// currentMode: NORMAL or SHELTERING
	currentMode uint8

	// PManager is a wrapper Storage with a memory cache.
	// It can switch between on-cache mode and must-persist mode according to its fsync flag.
	PManager adaptive.PersistentManager
}

// start prepares and starts raftNode in a new goroutine. It is no longer safe
// to modify the fields after it has been started.
func (an *AdaNode) start(rh *raftReadyHandler) {
	internalTimeout := time.Second

	go func() {
		defer an.onStop()
		isLead := false

		for {
			select {
			case <-an.ticker.C:
				an.tick()
			case rd := <-an.Ready():
				if rd.SoftState != nil {
					newLeader := rd.SoftState.Lead != raft.None && rh.getLead() != rd.SoftState.Lead
					if newLeader {
						leaderChanges.Inc()
					}

					if rd.SoftState.Lead == raft.None {
						hasLeader.Set(0)
					} else {
						hasLeader.Set(1)
					}

					rh.updateLead(rd.SoftState.Lead)
					isLead = rd.RaftState == raft.StateLeader
					if isLead {
						isLeader.Set(1)
					} else {
						isLeader.Set(0)
					}
					rh.updateLeadership(newLeader)
					an.td.Reset()

					// refresh PeerMonitor's state
					an.updatePeerMonitorFromReady(rd)
				}

				if len(rd.ReadStates) != 0 {
					select {
					case an.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
					case <-time.After(internalTimeout):
						if an.lg != nil {
							an.lg.Warn("timed out sending read state", zap.Duration("timeout", internalTimeout))
						} else {
							plog.Warningf("timed out sending read state")
						}
					case <-an.stopped:
						return
					}
				}

				notifyc := make(chan struct{}, 1)
				ap := apply{
					entries:  rd.CommittedEntries,
					snapshot: rd.Snapshot,
					notifyc:  notifyc,
				}

				updateCommittedIndex(&ap, rh)

				select {
				case an.applyc <- ap:
				case <-an.stopped:
					return
				}

				// the leader can write to its disk in parallel with replicating to the followers and them
				// writing to their disks.
				// For more details, check raft thesis 10.2.1
				if isLead {
					// gofail: var raftBeforeLeaderSend struct{}
					an.transport.Send(an.processMessages(rd.Messages))
					isCritical := an.PeerMonitor.IsCritical()

					if isCritical && an.currentMode == NORMAL {
						an.currentMode = SHELTERING
						oldStrategy := an.PManager.GetStrategy()
						err := an.PManager.SetStrategy(&adaptive.PersistentStrategy{
							Fsync:             true,
							MaxLocalCacheSize: oldStrategy.MaxLocalCacheSize,
							CachePreserveTime: oldStrategy.CachePreserveTime,
						})

						if err != nil {
							if an.lg != nil {
								an.lg.Fatal(
									"failed to transform the mode of PersistentManager",
									zap.Error(err),
									zap.Bool("is-leader", true),
									zap.String("mode", "NORMAL -> SHELTERING"),
									zap.Int("pm-local-cache-size", oldStrategy.MaxLocalCacheSize),
									zap.String("pm-cache-preserve-time", oldStrategy.CachePreserveTime.String()),
								)
							} else {
								plog.Fatalf("failed to transform the mode of PersistentManager: %v", err)
							}
						}
					} else if !isCritical && an.currentMode == SHELTERING {
						an.currentMode = NORMAL
						oldStrategy := an.PManager.GetStrategy()
						err := an.PManager.SetStrategy(&adaptive.PersistentStrategy{
							Fsync:             false,
							MaxLocalCacheSize: oldStrategy.MaxLocalCacheSize,
							CachePreserveTime: oldStrategy.CachePreserveTime,
						})

						if err != nil {
							if an.lg != nil {
								an.lg.Fatal(
									"failed to transform the mode of PersistentManager",
									zap.Error(err),
									zap.Bool("is-leader", true),
									zap.String("mode", "SHELTERING -> NORMAL"),
									zap.Int("pm-local-cache-size", oldStrategy.MaxLocalCacheSize),
									zap.String("pm-cache-preserve-time", oldStrategy.CachePreserveTime.String()),
								)
							} else {
								plog.Fatalf("failed to transform the mode of PersistentManager: %v", err)
							}
						}
					}
				}

				if err := an.PManager.Save(rd.HardState, rd.Entries); err != nil {
					if an.lg != nil {
						s := an.PManager.GetStrategy()
						an.lg.Fatal(
							"failed to save Raft hard state and entries",
							zap.Error(err),
							zap.String("mode", "NORMAL -> SHELTERING"),
							zap.Int("pm-local-cache-size", s.MaxLocalCacheSize),
							zap.String("pm-cache-preserve-time", s.CachePreserveTime.String()),
						)
					} else {
						plog.Fatalf("raft save state and entries error: %v", err)
					}
				}

				if !raft.IsEmptyHardState(rd.HardState) {
					proposalsCommitted.Set(float64(rd.HardState.Commit))
				}
				// gofail: var raftAfterSave struct{}

				// TODO(similtylers): deal with snapshot
				if !raft.IsEmptySnap(rd.Snapshot) {
					// gofail: var raftBeforeSaveSnap struct{}
					if err := an.storage.SaveSnap(rd.Snapshot); err != nil {
						if an.lg != nil {
							an.lg.Fatal("failed to save Raft snapshot", zap.Error(err))
						} else {
							plog.Fatalf("raft save snapshot error: %v", err)
						}
					}
					// etcdserver now claim the snapshot has been persisted onto the disk
					notifyc <- struct{}{}

					// gofail: var raftAfterSaveSnap struct{}
					an.raftStorage.ApplySnapshot(rd.Snapshot)
					if an.lg != nil {
						an.lg.Info("applied incoming Raft snapshot", zap.Uint64("snapshot-index", rd.Snapshot.Metadata.Index))
					} else {
						plog.Infof("raft applied incoming snapshot at index %d", rd.Snapshot.Metadata.Index)
					}
					// gofail: var raftAfterApplySnap struct{}
				}

				an.raftStorage.Append(rd.Entries)

				if !isLead {
					// finish processing incoming messages before we signal raftdone chan
					msgs := an.processMessages(rd.Messages)

					// now unblocks 'applyAll' that waits on Raft log disk writes before triggering snapshots
					notifyc <- struct{}{}

					// Candidate or follower needs to wait for all pending configuration
					// changes to be applied before sending messages.
					// Otherwise we might incorrectly count votes (e.g. votes from removed members).
					// Also slow machine's follower raft-layer could proceed to become the leader
					// on its own single-node cluster, before apply-layer applies the config change.
					// We simply wait for ALL pending entries to be applied for now.
					// We might improve this later on if it causes unnecessary long blocking issues.
					waitApply := false
					for _, ent := range rd.CommittedEntries {
						if ent.Type == raftpb.EntryConfChange {
							waitApply = true
							break
						}
					}
					if waitApply {
						// blocks until 'applyAll' calls 'applyWait.Trigger'
						// to be in sync with scheduled config-change job
						// (assume notifyc has cap of 1)
						select {
						case notifyc <- struct{}{}:
						case <-an.stopped:
							return
						}
					}

					// gofail: var raftBeforeFollowerSend struct{}
					an.transport.Send(msgs)

					isCritical := an.PeerMonitor.IsCritical()

					if isCritical && an.currentMode == NORMAL {
						an.currentMode = SHELTERING
						oldStrategy := an.PManager.GetStrategy()
						err := an.PManager.SetStrategy(&adaptive.PersistentStrategy{
							Fsync:             true,
							MaxLocalCacheSize: oldStrategy.MaxLocalCacheSize,
							CachePreserveTime: oldStrategy.CachePreserveTime,
						})

						if err != nil {
							if an.lg != nil {
								an.lg.Fatal(
									"failed to transform the mode of PersistentManager",
									zap.Error(err),
									zap.Bool("is-leader", false),
									zap.String("mode", "NORMAL -> SHELTERING"),
									zap.Int("pm-local-cache-size", oldStrategy.MaxLocalCacheSize),
									zap.String("pm-cache-preserve-time", oldStrategy.CachePreserveTime.String()),
								)
							} else {
								plog.Fatalf("failed to transform the mode of PersistentManager: %v", err)
							}
						}

						// flush all the cached
						if err := an.PManager.Flush(); err != nil {
							if an.lg != nil {
								s := an.PManager.GetStrategy()
								an.lg.Fatal(
									"failed to flush cached hardState and entries",
									zap.Error(err),
									zap.String("mode", "NORMAL -> SHELTERING"),
									zap.Int("pm-local-cache-size", s.MaxLocalCacheSize),
									zap.String("pm-cache-preserve-time", s.CachePreserveTime.String()),
								)
							} else {
								plog.Fatalf("flush cached hardState and entries error: %v", err)
							}
						}
					} else if !isCritical && an.currentMode == SHELTERING {
						an.currentMode = NORMAL
						oldStrategy := an.PManager.GetStrategy()
						err := an.PManager.SetStrategy(&adaptive.PersistentStrategy{
							Fsync:             false,
							MaxLocalCacheSize: oldStrategy.MaxLocalCacheSize,
							CachePreserveTime: oldStrategy.CachePreserveTime,
						})

						if err != nil {
							if an.lg != nil {
								an.lg.Fatal(
									"failed to transform the mode of PersistentManager",
									zap.Error(err),
									zap.Bool("is-leader", false),
									zap.String("mode", "SHELTERING -> NORMAL"),
									zap.Int("pm-local-cache-size", oldStrategy.MaxLocalCacheSize),
									zap.String("pm-cache-preserve-time", oldStrategy.CachePreserveTime.String()),
								)
							} else {
								plog.Fatalf("failed to transform the mode of PersistentManager: %v", err)
							}
						}
					}
				} else {
					// leader already processed 'MsgSnap' and signaled
					notifyc <- struct{}{}
				}

				an.Advance()
			case <-an.stopped:
				return
			}
		}
	}()
}

func (an *AdaNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if an.isIDRemoved(ms[i].To) {
			ms[i].To = 0
		}

		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {
			// There are two separate data store: the store for v2, and the KV for v3.
			// The msgSnap only contains the most recent snapshot of store without KV.
			// So we need to redirect the msgSnap to etcd server main loop for merging in the
			// current store snapshot and KV snapshot.
			select {
			case an.msgSnapC <- ms[i]:
			default:
				// drop msgSnap if the inflight chan if full.
			}
			ms[i].To = 0
		}
		if ms[i].Type == raftpb.MsgHeartbeat {
			an.PeerMonitor.Perceive(ms[i].To, false)
			ok, exceed := an.td.Observe(ms[i].To)
			if !ok {
				// TODO: limit request rate.
				if an.lg != nil {
					an.lg.Warn(
						"leader failed to send out heartbeat on time; took too long, leader is overloaded likely from slow disk",
						zap.String("to", fmt.Sprintf("%x", ms[i].To)),
						zap.Duration("heartbeat-interval", an.heartbeat),
						zap.Duration("expected-duration", 2*an.heartbeat),
						zap.Duration("exceeded-duration", exceed),
					)
				} else {
					plog.Warningf("failed to send out heartbeat on time (exceeded the %v timeout for %v, to %x)", an.heartbeat, exceed, ms[i].To)
					plog.Warningf("server is likely overloaded")
				}
				heartbeatSendFailures.Inc()
			}
		}
	}
	return ms
}

func (an *AdaNode) updatePeerMonitorFromReady(rd raft.Ready) {
	if rd.RaftState == raft.StateLeader || rd.RaftState == raft.StateFollower {
		critical := an.PeerMonitor.IsCritical()
		pConfig := &adaptive.PerceptibleConfig{
			State:    rd.RaftState,
			Leader:   rd.SoftState.Lead,
			Self:     an.self,
			Critical: critical,
			Peers:    an.peers,
		}

		if err := an.PeerMonitor.SetConfig(pConfig); err != nil {
			if an.lg != nil {
				an.lg.Fatal("failed to refresh PeerMonitor", zap.Error(err))
			} else {
				plog.Fatalf("PeerMonitor refreshing error: %v", err)
			}
		}
	} else {
		pConfig := &adaptive.PerceptibleConfig{
			State:    rd.RaftState,
			Leader:   rd.SoftState.Lead,
			Self:     an.self,
			Critical: true,
			Peers:    an.peers,
		}

		if err := an.PeerMonitor.SetConfig(pConfig); err != nil {
			if an.lg != nil {
				an.lg.Fatal("failed to refresh PeerMonitor", zap.Error(err))
			} else {
				plog.Fatalf("PeerMonitor refreshing error: %v", err)
			}
		}
	}

}

func NewAdaNode(r raftNode, peers []uint64) *AdaNode {
	monitor, err := adaptive.NewSaucrMonitor(r.lg, 5, &adaptive.PerceptibleConfig{
		State:    raft.StateFollower,
		Leader:   raft.None,
		Critical: false,
		Peers:    peers,
	})

	if err != nil {
		if r.lg != nil {
			r.lg.Fatal("AdaNode instantiation failed", zap.Error(err))
		}
		return nil
	}

	return &AdaNode{
		raftNode:    r,
		peers:       peers,
		self:        0,
		PeerMonitor: monitor,
		currentMode: NORMAL,
		PManager:    NewLocalDisk(r.lg, r.storage, &adaptive.PersistentConfig{Strategy: adaptive.DefaultStrategy}),
	}
}

package etcdserver

import (
	"context"
	"fmt"
	"github.com/coreos/go-semver/semver"
	"github.com/dustin/go-humanize"
	"go.etcd.io/etcd/adaptive"
	"go.etcd.io/etcd/auth"
	"go.etcd.io/etcd/etcdserver/api"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/etcdserver/api/v2discovery"
	"go.etcd.io/etcd/etcdserver/api/v2http/httptypes"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/etcdserver/api/v2store"
	"go.etcd.io/etcd/etcdserver/api/v3compactor"
	"go.etcd.io/etcd/mvcc"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/idutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/etcdserver/api/membership"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/pkg/schedule"
	"go.etcd.io/etcd/pkg/wait"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/version"
	"go.uber.org/zap"
)

type EtcdSaucrServer struct {
	*EtcdServer

	srn *SaucrRaftNode
}

func (s *EtcdSaucrServer) Start() {
	s.start()
	s.goAttach(func() { s.adjustTicks() })
	s.goAttach(func() { s.publish(s.Cfg.ReqTimeout()) })
	s.goAttach(s.purgeFile)
	s.goAttach(func() { monitorFileDescriptor(s.getLogger(), s.stopping) })
	s.goAttach(s.monitorVersions)
	s.goAttach(s.linearizableReadLoop)
	s.goAttach(s.monitorKVHash)
}

func (s *EtcdSaucrServer) start() {
	lg := s.getLogger()

	if s.Cfg.SnapshotCount == 0 {
		if lg != nil {
			lg.Info(
				"updating snapshot-count to default",
				zap.Uint64("given-snapshot-count", s.Cfg.SnapshotCount),
				zap.Uint64("updated-snapshot-count", DefaultSnapshotCount),
			)
		} else {
			plog.Infof("set snapshot count to default %d", DefaultSnapshotCount)
		}
		s.Cfg.SnapshotCount = DefaultSnapshotCount
	}
	if s.Cfg.SnapshotCatchUpEntries == 0 {
		if lg != nil {
			lg.Info(
				"updating snapshot catch-up entries to default",
				zap.Uint64("given-snapshot-catchup-entries", s.Cfg.SnapshotCatchUpEntries),
				zap.Uint64("updated-snapshot-catchup-entries", DefaultSnapshotCatchUpEntries),
			)
		}
		s.Cfg.SnapshotCatchUpEntries = DefaultSnapshotCatchUpEntries
	}

	s.w = wait.New()
	s.applyWait = wait.NewTimeList()
	s.done = make(chan struct{})
	s.stop = make(chan struct{})
	s.stopping = make(chan struct{})
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.readwaitc = make(chan struct{}, 1)
	s.readNotifier = newNotifier()
	s.leaderChanged = make(chan struct{})
	if s.ClusterVersion() != nil {
		if lg != nil {
			lg.Info(
				"starting etcd server",
				zap.String("local-member-id", s.ID().String()),
				zap.String("local-server-version", version.Version),
				zap.String("cluster-id", s.Cluster().ID().String()),
				zap.String("cluster-version", version.Cluster(s.ClusterVersion().String())),
			)
		} else {
			plog.Infof("starting server... [version: %v, cluster version: %v]", version.Version, version.Cluster(s.ClusterVersion().String()))
		}
		membership.ClusterVersionMetrics.With(prometheus.Labels{"cluster_version": version.Cluster(s.ClusterVersion().String())}).Set(1)
	} else {
		if lg != nil {
			lg.Info(
				"starting etcd server",
				zap.String("local-member-id", s.ID().String()),
				zap.String("local-server-version", version.Version),
				zap.String("cluster-version", "to_be_decided"),
			)
		} else {
			plog.Infof("starting server... [version: %v, cluster version: to_be_decided]", version.Version)
		}
	}

	// TODO: if this is an empty log, writes all peer infos
	// into the first entry
	go s.run()
}

// EtcdSaucrServer's routine runs SaucrRaftNode rather than raftNode
func (s *EtcdSaucrServer) run() {
	lg := s.getLogger()

	sn, err := s.r.raftStorage.Snapshot()
	if err != nil {
		if lg != nil {
			lg.Panic("failed to get snapshot from Raft storage", zap.Error(err))
		} else {
			plog.Panicf("get snapshot from raft storage error: %v", err)
		}
	}

	// asynchronously accept apply packets, dispatch progress in-order
	sched := schedule.NewFIFOScheduler()

	var (
		smu   sync.RWMutex
		syncC <-chan time.Time
	)
	setSyncC := func(ch <-chan time.Time) {
		smu.Lock()
		syncC = ch
		smu.Unlock()
	}
	getSyncC := func() (ch <-chan time.Time) {
		smu.RLock()
		ch = syncC
		smu.RUnlock()
		return
	}
	rh := &raftReadyHandler{
		getLead:    func() (lead uint64) { return s.getLead() },
		updateLead: func(lead uint64) { s.setLead(lead) },
		updateLeadership: func(newLeader bool) {
			if !s.isLeader() {
				if s.lessor != nil {
					s.lessor.Demote()
				}
				if s.compactor != nil {
					s.compactor.Pause()
				}
				setSyncC(nil)
			} else {
				if newLeader {
					t := time.Now()
					s.leadTimeMu.Lock()
					s.leadElectedTime = t
					s.leadTimeMu.Unlock()
				}
				setSyncC(s.SyncTicker.C)
				if s.compactor != nil {
					s.compactor.Resume()
				}
			}
			if newLeader {
				s.leaderChangedMu.Lock()
				lc := s.leaderChanged
				s.leaderChanged = make(chan struct{})
				close(lc)
				s.leaderChangedMu.Unlock()
			}
			// TODO: remove the nil checking
			// current test utility does not provide the stats
			if s.stats != nil {
				s.stats.BecomeLeader()
			}
		},
		updateCommittedIndex: func(ci uint64) {
			cci := s.getCommittedIndex()
			if ci > cci {
				s.setCommittedIndex(ci)
			}
		},
	}

	// replace SaucrRaftNode's routine for raftNode's routine
	s.srn.start(rh)

	ep := etcdProgress{
		confState: sn.Metadata.ConfState,
		snapi:     sn.Metadata.Index,
		appliedt:  sn.Metadata.Term,
		appliedi:  sn.Metadata.Index,
	}

	defer func() {
		s.wgMu.Lock() // block concurrent waitgroup adds in goAttach while stopping
		close(s.stopping)
		s.wgMu.Unlock()
		s.cancel()

		sched.Stop()

		// wait for gouroutines before closing raft so wal stays open
		s.wg.Wait()

		s.SyncTicker.Stop()

		// must stop raft after scheduler-- etcdserver can leak rafthttp pipelines
		// by adding a peer after raft stops the transport
		s.srn.stop()

		// kv, lessor and backend can be nil if running without v3 enabled
		// or running unit tests.
		if s.lessor != nil {
			s.lessor.Stop()
		}
		if s.kv != nil {
			s.kv.Close()
		}
		if s.authStore != nil {
			s.authStore.Close()
		}
		if s.be != nil {
			s.be.Close()
		}
		if s.compactor != nil {
			s.compactor.Stop()
		}
		close(s.done)
	}()

	var expiredLeaseC <-chan []*lease.Lease
	if s.lessor != nil {
		expiredLeaseC = s.lessor.ExpiredLeasesC()
	}

	for {
		select {
		case ap := <-s.srn.apply():
			f := func(context.Context) { s.applyAll(&ep, &ap) }
			sched.Schedule(f)
		case leases := <-expiredLeaseC:
			s.goAttach(func() {
				// Increases throughput of expired leases deletion process through parallelization
				c := make(chan struct{}, maxPendingRevokes)
				for _, lease := range leases {
					select {
					case c <- struct{}{}:
					case <-s.stopping:
						return
					}
					lid := lease.ID
					s.goAttach(func() {
						ctx := s.authStore.WithRoot(s.ctx)
						_, lerr := s.LeaseRevoke(ctx, &pb.LeaseRevokeRequest{ID: int64(lid)})
						if lerr == nil {
							leaseExpired.Inc()
						} else {
							if lg != nil {
								lg.Warn(
									"failed to revoke lease",
									zap.String("lease-id", fmt.Sprintf("%016x", lid)),
									zap.Error(lerr),
								)
							} else {
								plog.Warningf("failed to revoke %016x (%q)", lid, lerr.Error())
							}
						}

						<-c
					})
				}
			})
		case err := <-s.errorc:
			if lg != nil {
				lg.Warn("server error", zap.Error(err))
				lg.Warn("data-dir used by this member must be removed")
			} else {
				plog.Errorf("%s", err)
				plog.Infof("the data-dir used by this member must be removed.")
			}
			return
		case <-getSyncC():
			if s.v2store.HasTTLKeys() {
				s.sync(s.Cfg.ReqTimeout())
			}
		case <-s.stop:
			return
		}
	}
}

func (s *EtcdSaucrServer) Process(ctx context.Context, m raftpb.Message) error {
	if s.cluster.IsIDRemoved(types.ID(m.From)) {
		if lg := s.getLogger(); lg != nil {
			lg.Warn(
				"rejected Raft message from removed member",
				zap.String("local-member-id", s.ID().String()),
				zap.String("removed-member-id", types.ID(m.From).String()),
			)
		} else {
			plog.Warningf("reject message from removed member %s", types.ID(m.From).String())
		}
		return httptypes.NewHTTPError(http.StatusForbidden, "cannot process message from removed member")
	}

	// if it involves in MsgSaucr/MsgSaucrResp, intercept it
	if isMsgSaucr(m.Type) {
		s.srn.ReceiveMsgSaucr(m)
		return nil
	} else if isMsgSaucrResp(m.Type) {
		s.srn.PeerMonitor.Perceive(m.From, true)
		return nil
	}

	if m.Type == raftpb.MsgApp {
		s.stats.RecvAppendReq(types.ID(m.From).String(), m.Size())
	}
	return s.r.Step(ctx, m)
}

func isMsgSaucr(mType raftpb.MessageType) bool {
	return mType == raftpb.MsgSaucrNormal || mType == raftpb.MsgSaucrSheltering
}

func isMsgSaucrResp(mType raftpb.MessageType) bool {
	return mType == raftpb.MsgSaucrNormalResp || mType == raftpb.MsgSaucrShelteringResp
}

func NewEtcdSaucrServer(cfg ServerConfig, sCfg *SaucrConfig) (srv *EtcdSaucrServer, err error) {
	if sCfg == nil {
		sCfg = DefaultSaucrConfig
	}

	st := v2store.New(StoreClusterPrefix, StoreKeysPrefix)

	var (
		w    *wal.WAL
		n    raft.Node
		s    *raft.MemoryStorage
		id   types.ID
		cl   *membership.RaftCluster
		mode SaucrMode
	)

	if cfg.MaxRequestBytes > recommendedMaxRequestBytes {
		if cfg.Logger != nil {
			cfg.Logger.Warn(
				"exceeded recommended request limit",
				zap.Uint("max-request-bytes", cfg.MaxRequestBytes),
				zap.String("max-request-size", humanize.Bytes(uint64(cfg.MaxRequestBytes))),
				zap.Int("recommended-request-bytes", recommendedMaxRequestBytes),
				zap.String("recommended-request-size", humanize.Bytes(uint64(recommendedMaxRequestBytes))),
			)
		} else {
			plog.Warningf("MaxRequestBytes %v exceeds maximum recommended size %v", cfg.MaxRequestBytes, recommendedMaxRequestBytes)
		}
	}

	if terr := fileutil.TouchDirAll(cfg.DataDir); terr != nil {
		return nil, fmt.Errorf("cannot access data directory: %v", terr)
	}

	haveWAL := wal.Exist(cfg.WALDir())

	if err = fileutil.TouchDirAll(cfg.SnapDir()); err != nil {
		if cfg.Logger != nil {
			cfg.Logger.Fatal(
				"failed to create snapshot directory",
				zap.String("path", cfg.SnapDir()),
				zap.Error(err),
			)
		} else {
			plog.Fatalf("create snapshot directory error: %v", err)
		}
	}
	ss := snap.New(cfg.Logger, cfg.SnapDir())

	bepath := cfg.backendPath()
	beExist := fileutil.Exist(bepath)
	be := openBackend(cfg)

	defer func() {
		if err != nil {
			be.Close()
		}
	}()

	prt, err := rafthttp.NewRoundTripper(cfg.PeerTLSInfo, cfg.peerDialTimeout())
	if err != nil {
		return nil, err
	}
	var (
		remotes  []*membership.Member
		snapshot *raftpb.Snapshot
	)

	switch {
	case !haveWAL && !cfg.NewCluster:
		if err = cfg.VerifyJoinExisting(); err != nil {
			return nil, err
		}
		cl, err = membership.NewClusterFromURLsMap(cfg.Logger, cfg.InitialClusterToken, cfg.InitialPeerURLsMap)
		if err != nil {
			return nil, err
		}
		existingCluster, gerr := GetClusterFromRemotePeers(cfg.Logger, getRemotePeerURLs(cl, cfg.Name), prt)
		if gerr != nil {
			return nil, fmt.Errorf("cannot fetch cluster info from peer urls: %v", gerr)
		}
		if err = membership.ValidateClusterAndAssignIDs(cfg.Logger, cl, existingCluster); err != nil {
			return nil, fmt.Errorf("error validating peerURLs %s: %v", existingCluster, err)
		}
		if !isCompatibleWithCluster(cfg.Logger, cl, cl.MemberByName(cfg.Name).ID, prt) {
			return nil, fmt.Errorf("incompatible with current running cluster")
		}

		remotes = existingCluster.Members()
		cl.SetID(types.ID(0), existingCluster.ID())
		cl.SetStore(st)
		cl.SetBackend(be)
		id, n, s, w = startNode(cfg, cl, nil)
		cl.SetID(id, existingCluster.ID())

	case !haveWAL && cfg.NewCluster:
		if err = cfg.VerifyBootstrap(); err != nil {
			return nil, err
		}
		cl, err = membership.NewClusterFromURLsMap(cfg.Logger, cfg.InitialClusterToken, cfg.InitialPeerURLsMap)
		if err != nil {
			return nil, err
		}
		m := cl.MemberByName(cfg.Name)
		if isMemberBootstrapped(cfg.Logger, cl, cfg.Name, prt, cfg.bootstrapTimeout()) {
			return nil, fmt.Errorf("member %s has already been bootstrapped", m.ID)
		}
		if cfg.ShouldDiscover() {
			var str string
			str, err = v2discovery.JoinCluster(cfg.Logger, cfg.DiscoveryURL, cfg.DiscoveryProxy, m.ID, cfg.InitialPeerURLsMap.String())
			if err != nil {
				return nil, &DiscoveryError{Op: "join", Err: err}
			}
			var urlsmap types.URLsMap
			urlsmap, err = types.NewURLsMap(str)
			if err != nil {
				return nil, err
			}
			if checkDuplicateURL(urlsmap) {
				return nil, fmt.Errorf("discovery cluster %s has duplicate url", urlsmap)
			}
			if cl, err = membership.NewClusterFromURLsMap(cfg.Logger, cfg.InitialClusterToken, urlsmap); err != nil {
				return nil, err
			}
		}
		cl.SetStore(st)
		cl.SetBackend(be)
		id, n, s, w = startNode(cfg, cl, cl.MemberIDs())
		cl.SetID(id, cl.ID())

	case haveWAL:
		if err = fileutil.IsDirWriteable(cfg.MemberDir()); err != nil {
			return nil, fmt.Errorf("cannot write to member directory: %v", err)
		}

		if err = fileutil.IsDirWriteable(cfg.WALDir()); err != nil {
			return nil, fmt.Errorf("cannot write to WAL directory: %v", err)
		}

		if cfg.ShouldDiscover() {
			if cfg.Logger != nil {
				cfg.Logger.Warn(
					"discovery token is ignored since cluster already initialized; valid logs are found",
					zap.String("wal-dir", cfg.WALDir()),
				)
			} else {
				plog.Warningf("discovery token ignored since a cluster has already been initialized. Valid log found at %q", cfg.WALDir())
			}
		}
		snapshot, err = ss.Load()
		if err != nil && err != snap.ErrNoSnapshot {
			return nil, err
		}
		if snapshot != nil {
			if err = st.Recovery(snapshot.Data); err != nil {
				if cfg.Logger != nil {
					cfg.Logger.Panic("failed to recover from snapshot")
				} else {
					plog.Panicf("recovered store from snapshot error: %v", err)
				}
			}

			if cfg.Logger != nil {
				cfg.Logger.Info(
					"recovered v2 store from snapshot",
					zap.Uint64("snapshot-index", snapshot.Metadata.Index),
					zap.String("snapshot-size", humanize.Bytes(uint64(snapshot.Size()))),
				)
			} else {
				plog.Infof("recovered store from snapshot at index %d", snapshot.Metadata.Index)
			}

			if be, err = recoverSnapshotBackend(cfg, be, *snapshot); err != nil {
				if cfg.Logger != nil {
					cfg.Logger.Panic("failed to recover v3 backend from snapshot", zap.Error(err))
				} else {
					plog.Panicf("recovering backend from snapshot error: %v", err)
				}
			}
			if cfg.Logger != nil {
				s1, s2 := be.Size(), be.SizeInUse()
				cfg.Logger.Info(
					"recovered v3 backend from snapshot",
					zap.Int64("backend-size-bytes", s1),
					zap.String("backend-size", humanize.Bytes(uint64(s1))),
					zap.Int64("backend-size-in-use-bytes", s2),
					zap.String("backend-size-in-use", humanize.Bytes(uint64(s2))),
				)
			}
		}

		if !cfg.ForceNewCluster {
			id, cl, n, s, w = restartNode(cfg, snapshot)
		} else {
			id, cl, n, s, w = restartAsStandaloneNode(cfg, snapshot)
		}

		cl.SetStore(st)
		cl.SetBackend(be)
		cl.Recover(api.UpdateCapability)
		if cl.Version() != nil && !cl.Version().LessThan(semver.Version{Major: 3}) && !beExist {
			os.RemoveAll(bepath)
			return nil, fmt.Errorf("database file (%v) of the backend is missing", bepath)
		}

	default:
		return nil, fmt.Errorf("unsupported bootstrap config")
	}

	if terr := fileutil.TouchDirAll(cfg.MemberDir()); terr != nil {
		return nil, fmt.Errorf("cannot access member directory: %v", terr)
	}

	// assembling saucr-related configurations

	peerIds := cl.MemberIDs()
	peers := make([]uint64, len(peerIds))
	for i := 0; i < len(peers); i++ {
		peers[i] = uint64(peerIds[i])
	}

	mode = sCfg.InitMode

	pMonitorCfg := &adaptive.PerceptibleConfig{
		State:    raft.StateFollower,
		Leader:   raft.None,
		Self:     uint64(id),
		Critical: mode.IsCritical(),
		Peers:    peers,
	}

	pManagerStg := &adaptive.PersistentStrategy{
		Fsync:             mode.IsFsync(),
		MaxLocalCacheSize: sCfg.MaxLocalCacheSize,
		CachePreserveTime: sCfg.CachePreserveTime,
	}

	sstats := stats.NewServerStats(cfg.Name, id.String())
	lstats := stats.NewLeaderStats(id.String())

	heartbeat := time.Duration(cfg.TickMs) * time.Millisecond
	srv = &EtcdSaucrServer{
		EtcdServer: &EtcdServer{
			readych:     make(chan struct{}),
			Cfg:         cfg,
			lgMu:        new(sync.RWMutex),
			lg:          cfg.Logger,
			errorc:      make(chan error, 1),
			v2store:     st,
			snapshotter: ss,
			r: *newRaftNode(
				raftNodeConfig{
					lg:          cfg.Logger,
					isIDRemoved: func(id uint64) bool { return cl.IsIDRemoved(types.ID(id)) },
					Node:        n,
					heartbeat:   heartbeat,
					raftStorage: s,
					storage:     NewStorage(w, ss),
				},
			),
			id:               id,
			attributes:       membership.Attributes{Name: cfg.Name, ClientURLs: cfg.ClientURLs.StringSlice()},
			cluster:          cl,
			stats:            sstats,
			lstats:           lstats,
			SyncTicker:       time.NewTicker(500 * time.Millisecond),
			peerRt:           prt,
			reqIDGen:         idutil.NewGenerator(uint16(id), time.Now()),
			forceVersionC:    make(chan struct{}),
			AccessController: &AccessController{CORS: cfg.CORS, HostWhitelist: cfg.HostWhitelist},
		},
	}

	// todo: check if a wrapper is necessary
	srv.srn = NewSaucrRaftNode(&(srv.r), pMonitorCfg, pManagerStg, sCfg)
	srv.srn.clusterUpdater = func() []uint64 {
		mIds := srv.cluster.MemberIDs()
		peers := make([]uint64, len(mIds))
		for i := 0; i < len(mIds); i++ {
			peers[i] = uint64(mIds[i])
		}
		return peers
	}
	srv.r.storage = srv.srn.PManager

	serverID.With(prometheus.Labels{"server_id": id.String()}).Set(1)

	srv.applyV2 = &applierV2store{store: srv.v2store, cluster: srv.cluster}

	srv.be = be
	minTTL := time.Duration((3*cfg.ElectionTicks)/2) * heartbeat

	// always recover lessor before kv. When we recover the mvcc.KV it will reattach keys to its leases.
	// If we recover mvcc.KV first, it will attach the keys to the wrong lessor before it recovers.
	srv.lessor = lease.NewLessor(
		srv.getLogger(),
		srv.be,
		lease.LessorConfig{
			MinLeaseTTL:                int64(math.Ceil(minTTL.Seconds())),
			CheckpointInterval:         cfg.LeaseCheckpointInterval,
			ExpiredLeasesRetryInterval: srv.Cfg.ReqTimeout(),
		})
	srv.kv = mvcc.New(srv.getLogger(), srv.be, srv.lessor, &srv.consistIndex, mvcc.StoreConfig{CompactionBatchLimit: cfg.CompactionBatchLimit})
	if beExist {
		kvindex := srv.kv.ConsistentIndex()
		// TODO: remove kvindex != 0 checking when we do not expect users to upgrade
		// etcd from pre-3.0 release.
		if snapshot != nil && kvindex < snapshot.Metadata.Index {
			if kvindex != 0 {
				return nil, fmt.Errorf("database file (%v index %d) does not match with snapshot (index %d)", bepath, kvindex, snapshot.Metadata.Index)
			}
			if cfg.Logger != nil {
				cfg.Logger.Warn(
					"consistent index was never saved",
					zap.Uint64("snapshot-index", snapshot.Metadata.Index),
				)
			} else {
				plog.Warningf("consistent index never saved (snapshot index=%d)", snapshot.Metadata.Index)
			}
		}
	}
	newSrv := srv // since srv == nil in defer if srv is returned as nil
	defer func() {
		// closing backend without first closing kv can cause
		// resumed compactions to fail with closed tx errors
		if err != nil {
			newSrv.kv.Close()
		}
	}()

	srv.consistIndex.setConsistentIndex(srv.kv.ConsistentIndex())
	tp, err := auth.NewTokenProvider(cfg.Logger, cfg.AuthToken,
		func(index uint64) <-chan struct{} {
			return srv.applyWait.Wait(index)
		},
	)
	if err != nil {
		if cfg.Logger != nil {
			cfg.Logger.Warn("failed to create token provider", zap.Error(err))
		} else {
			plog.Errorf("failed to create token provider: %s", err)
		}
		return nil, err
	}
	srv.authStore = auth.NewAuthStore(srv.getLogger(), srv.be, tp, int(cfg.BcryptCost))
	if num := cfg.AutoCompactionRetention; num != 0 {
		srv.compactor, err = v3compactor.New(cfg.Logger, cfg.AutoCompactionMode, num, srv.kv, srv)
		if err != nil {
			return nil, err
		}
		srv.compactor.Run()
	}

	srv.applyV3Base = srv.newApplierV3Backend()
	if err = srv.restoreAlarms(); err != nil {
		return nil, err
	}

	if srv.Cfg.EnableLeaseCheckpoint {
		// setting checkpointer enables lease checkpoint feature.
		srv.lessor.SetCheckpointer(func(ctx context.Context, cp *pb.LeaseCheckpointRequest) {
			srv.raftRequestOnce(ctx, pb.InternalRaftRequest{LeaseCheckpoint: cp})
		})
	}

	// TODO: move transport initialization near the definition of remote
	tr := &rafthttp.Transport{
		Logger:      cfg.Logger,
		TLSInfo:     cfg.PeerTLSInfo,
		DialTimeout: cfg.peerDialTimeout(),
		ID:          id,
		URLs:        cfg.PeerURLs,
		ClusterID:   cl.ID(),
		Raft:        srv,
		Snapshotter: ss,
		ServerStats: sstats,
		LeaderStats: lstats,
		ErrorC:      srv.errorc,
	}
	if err = tr.Start(); err != nil {
		return nil, err
	}
	// add all remotes into transport
	for _, m := range remotes {
		if m.ID != id {
			tr.AddRemote(m.ID, m.PeerURLs)
		}
	}
	for _, m := range cl.Members() {
		if m.ID != id {
			tr.AddPeer(m.ID, m.PeerURLs)
		}
	}

	srv.r.transport = tr

	return srv, nil
}

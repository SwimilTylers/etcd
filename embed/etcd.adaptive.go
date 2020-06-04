package embed

import (
	"fmt"
	"go.etcd.io/etcd/etcdserver"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
)

// StartSaucrEtcd launches the etcd server and HTTP handlers for client/server communication.
// The returned Etcd.Server is not guaranteed to have joined the cluster. Wait
// on the Etcd.Server.ReadyNotify() channel to know when it completes and is ready for use.
func StartSaucrEtcd(inCfg *Config, saucrCfg *etcdserver.SaucrConfig) (e *Etcd, err error) {
	if err = inCfg.Validate(); err != nil {
		return nil, err
	}
	serving := false
	e = &Etcd{cfg: *inCfg, stopc: make(chan struct{})}
	cfg := &e.cfg
	defer func() {
		if e == nil || err == nil {
			return
		}
		if !serving {
			// errored before starting gRPC server for serveCtx.serversC
			for _, sctx := range e.sctxs {
				close(sctx.serversC)
			}
		}
		e.Close()
		e = nil
	}()

	if e.cfg.logger != nil {
		e.cfg.logger.Info(
			"configuring peer listeners",
			zap.Strings("listen-peer-urls", e.cfg.getLPURLs()),
		)
	}
	if e.Peers, err = configurePeerListeners(cfg); err != nil {
		return e, err
	}

	if e.cfg.logger != nil {
		e.cfg.logger.Info(
			"configuring client listeners",
			zap.Strings("listen-client-urls", e.cfg.getLCURLs()),
		)
	}
	if e.sctxs, err = configureClientListeners(cfg); err != nil {
		return e, err
	}

	for _, sctx := range e.sctxs {
		e.Clients = append(e.Clients, sctx.l)
	}

	var (
		urlsmap types.URLsMap
		token   string
	)
	memberInitialized := true
	if !isMemberInitialized(cfg) {
		memberInitialized = false
		urlsmap, token, err = cfg.PeerURLsMapAndToken("etcd")
		if err != nil {
			return e, fmt.Errorf("error setting up initial cluster: %v", err)
		}
	}

	// AutoCompactionRetention defaults to "0" if not set.
	if len(cfg.AutoCompactionRetention) == 0 {
		cfg.AutoCompactionRetention = "0"
	}
	autoCompactionRetention, err := parseCompactionRetention(cfg.AutoCompactionMode, cfg.AutoCompactionRetention)
	if err != nil {
		return e, err
	}

	backendFreelistType := parseBackendFreelistType(cfg.BackendFreelistType)

	srvcfg := etcdserver.ServerConfig{
		Name:                       cfg.Name,
		ClientURLs:                 cfg.ACUrls,
		PeerURLs:                   cfg.APUrls,
		DataDir:                    cfg.Dir,
		DedicatedWALDir:            cfg.WalDir,
		SnapshotCount:              cfg.SnapshotCount,
		SnapshotCatchUpEntries:     cfg.SnapshotCatchUpEntries,
		MaxSnapFiles:               cfg.MaxSnapFiles,
		MaxWALFiles:                cfg.MaxWalFiles,
		InitialPeerURLsMap:         urlsmap,
		InitialClusterToken:        token,
		DiscoveryURL:               cfg.Durl,
		DiscoveryProxy:             cfg.Dproxy,
		NewCluster:                 cfg.IsNewCluster(),
		PeerTLSInfo:                cfg.PeerTLSInfo,
		TickMs:                     cfg.TickMs,
		ElectionTicks:              cfg.ElectionTicks(),
		InitialElectionTickAdvance: cfg.InitialElectionTickAdvance,
		AutoCompactionRetention:    autoCompactionRetention,
		AutoCompactionMode:         cfg.AutoCompactionMode,
		QuotaBackendBytes:          cfg.QuotaBackendBytes,
		BackendBatchLimit:          cfg.BackendBatchLimit,
		BackendFreelistType:        backendFreelistType,
		BackendBatchInterval:       cfg.BackendBatchInterval,
		MaxTxnOps:                  cfg.MaxTxnOps,
		MaxRequestBytes:            cfg.MaxRequestBytes,
		StrictReconfigCheck:        cfg.StrictReconfigCheck,
		ClientCertAuthEnabled:      cfg.ClientTLSInfo.ClientCertAuth,
		AuthToken:                  cfg.AuthToken,
		BcryptCost:                 cfg.BcryptCost,
		CORS:                       cfg.CORS,
		HostWhitelist:              cfg.HostWhitelist,
		InitialCorruptCheck:        cfg.ExperimentalInitialCorruptCheck,
		CorruptCheckTime:           cfg.ExperimentalCorruptCheckTime,
		PreVote:                    cfg.PreVote,
		Logger:                     cfg.logger,
		LoggerConfig:               cfg.loggerConfig,
		LoggerCore:                 cfg.loggerCore,
		LoggerWriteSyncer:          cfg.loggerWriteSyncer,
		Debug:                      cfg.Debug,
		ForceNewCluster:            cfg.ForceNewCluster,
		EnableGRPCGateway:          cfg.EnableGRPCGateway,
		EnableLeaseCheckpoint:      cfg.ExperimentalEnableLeaseCheckpoint,
		CompactionBatchLimit:       cfg.ExperimentalCompactionBatchLimit,
	}
	print(e.cfg.logger, *cfg, srvcfg, memberInitialized)
	var saucr *etcdserver.EtcdSaucrServer
	if saucr, err = etcdserver.NewEtcdSaucrServer(srvcfg, saucrCfg); err != nil {
		return e, err
	}

	e.Server = saucr.EtcdServer

	// buffer channel so goroutines on closed connections won't wait forever
	e.errc = make(chan error, len(e.Peers)+len(e.Clients)+2*len(e.sctxs))

	// newly started member ("memberInitialized==false")
	// does not need corruption check
	if memberInitialized {
		if err = e.Server.CheckInitialHashKV(); err != nil {
			// set "EtcdServer" to nil, so that it does not block on "EtcdServer.Close()"
			// (nothing to close since rafthttp transports have not been started)
			e.Server = nil
			return e, err
		}
	}
	saucr.Start()

	if err = e.servePeers(); err != nil {
		return e, err
	}
	if err = e.serveClients(); err != nil {
		return e, err
	}
	if err = e.serveMetrics(); err != nil {
		return e, err
	}

	if e.cfg.logger != nil {
		e.cfg.logger.Info(
			"now serving peer/client/metrics",
			zap.String("local-member-id", e.Server.ID().String()),
			zap.Strings("initial-advertise-peer-urls", e.cfg.getAPURLs()),
			zap.Strings("listen-peer-urls", e.cfg.getLPURLs()),
			zap.Strings("advertise-client-urls", e.cfg.getACURLs()),
			zap.Strings("listen-client-urls", e.cfg.getLCURLs()),
			zap.Strings("listen-metrics-urls", e.cfg.getMetricsURLs()),
		)
	}
	serving = true
	return e, nil
}

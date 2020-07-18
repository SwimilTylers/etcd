package tests

import (
	"go.etcd.io/etcd/adaptive"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver"
	"go.uber.org/zap"
	"log"
)

func startOneSaucr(cluster *CDescriptor, idx int, sCfg *etcdserver.SaucrConfig) (*embed.Etcd, error) {
	cfg := cluster.GetConfig(idx, embed.ClusterStateFlagNew)
	return embed.StartSaucrEtcd(cfg, sCfg)
}

func restartOneSaucr(cluster *CDescriptor, idx int, sCfg *etcdserver.SaucrConfig) (*embed.Etcd, error) {
	cfg := cluster.GetConfig(idx, embed.ClusterStateFlagExisting)
	return embed.StartSaucrEtcd(cfg, sCfg)
}

var SaucrServerTestRunner = TestRunner{
	Name: "SaucrServerTestRunner",
	Start: func(c *CDescriptor, idx int) (*embed.Etcd, error) {
		return startOneSaucr(c, idx, GlobalRunnerConfigs["saucr"].(*etcdserver.SaucrConfig))
	},
	Restart: func(c *CDescriptor, idx int) (*embed.Etcd, error) {
		return restartOneSaucr(c, idx, GlobalRunnerConfigs["saucr"].(*etcdserver.SaucrConfig))
	},
	RunX: func(selected []int, scheduler Scheduler) {
		cluster, sCfg := GlobalRunnerConfigs["cx"].(*CDescriptor), GlobalRunnerConfigs["saucr"].(*etcdserver.SaucrConfig)

		s := make([]*embed.Etcd, len(selected))
		for i := 0; i < len(s); i++ {
			go func(idx int) {
				srv, err := startOneSaucr(cluster, selected[idx], sCfg)
				s[idx] = srv
				if err != nil {
					// if the server cannot init properly, stop the test immediately
					scheduler.err <- err
					scheduler.end <- struct{}{}
					return
				}
				<-srv.Server.ReadyNotify()
				scheduler.do(selected[idx], srv)
			}(i)
		}

		defer func() {
			for _, etcd := range s {
				etcd.Close()
			}
		}()

		logger, _ := GetSchedulerLogger()

		for {
			select {
			case e := <-scheduler.err:
				logger.Fatal("receive error from scheduler", zap.Error(e))
			case <-scheduler.end:
				logger.Info("SaucrServerTestRunner.RunX terminated!")
				return
			}
		}
	},
	Run1: func(scheduler Scheduler) {
		cluster, sCfg := GlobalRunnerConfigs["c1"].(*CDescriptor), GlobalRunnerConfigs["saucr"].(*etcdserver.SaucrConfig)

		srv, err := startOneSaucr(cluster, GlobalRunnerConfigs["standaloneIdx"].(int), sCfg)
		if err != nil {
			log.Fatal(err)
			return
		}
		go func() {
			<-srv.Server.ReadyNotify()
			scheduler.do(0, srv)
		}()

		defer srv.Close()

		logger, _ := GetSchedulerLogger()

		for {
			select {
			case e := <-scheduler.err:
				logger.Fatal("receive error from scheduler", zap.Error(e))
			case <-scheduler.end:
				logger.Info("NormalServerTestRunner.Run1 terminated!")
				return
			}
		}
	},
	Run3: func(scheduler Scheduler) {
		cluster, sCfg := GlobalRunnerConfigs["c3"].(*CDescriptor), GlobalRunnerConfigs["saucr"].(*etcdserver.SaucrConfig)

		s := make([]*embed.Etcd, 3)
		for i := 0; i < 3; i++ {
			go func(idx int) {
				srv, err := startOneSaucr(cluster, idx, sCfg)
				s[idx] = srv
				if err != nil {
					// if the server cannot init properly, stop the test immediately
					scheduler.err <- err
					scheduler.end <- struct{}{}
					return
				}
				<-srv.Server.ReadyNotify()
				scheduler.do(idx, srv)
			}(i)
		}

		defer func() {
			for _, etcd := range s {
				etcd.Close()
			}
		}()

		logger, _ := GetSchedulerLogger()

		for {
			select {
			case e := <-scheduler.err:
				logger.Fatal("receive error from scheduler", zap.Error(e))
			case <-scheduler.end:
				logger.Info("SaucrServerTestRunner.Run3 terminated!")
				return
			}
		}
	},
	Run5: func(scheduler Scheduler) {
		cluster, sCfg := GlobalRunnerConfigs["c5"].(*CDescriptor), GlobalRunnerConfigs["saucr"].(*etcdserver.SaucrConfig)

		s := make([]*embed.Etcd, 5)
		for i := 0; i < 5; i++ {
			go func(idx int) {
				srv, err := startOneSaucr(cluster, idx, sCfg)
				s[idx] = srv
				if err != nil {
					// if the server cannot init properly, stop the test immediately
					scheduler.err <- err
					scheduler.end <- struct{}{}
					return
				}
				<-srv.Server.ReadyNotify()
				scheduler.do(idx, srv)
			}(i)
		}

		defer func() {
			for _, etcd := range s {
				etcd.Close()
			}
		}()

		logger, _ := GetSchedulerLogger()

		for {
			select {
			case e := <-scheduler.err:
				logger.Fatal("receive error from scheduler", zap.Error(e))
			case <-scheduler.end:
				logger.Info("SaucrServerTestRunner.Run5 terminated!")
				return
			}
		}
	},
	Run7: func(scheduler Scheduler) {
		cluster, sCfg := GlobalRunnerConfigs["c7"].(*CDescriptor), GlobalRunnerConfigs["saucr"].(*etcdserver.SaucrConfig)

		s := make([]*embed.Etcd, 7)
		for i := 0; i < 7; i++ {
			go func(idx int) {
				srv, err := startOneSaucr(cluster, idx, sCfg)
				s[idx] = srv
				if err != nil {
					// if the server cannot init properly, stop the test immediately
					scheduler.err <- err
					scheduler.end <- struct{}{}
					return
				}
				<-srv.Server.ReadyNotify()
				scheduler.do(idx, srv)
			}(i)
		}

		defer func() {
			for _, etcd := range s {
				etcd.Close()
			}
		}()

		logger, _ := GetSchedulerLogger()

		for {
			select {
			case e := <-scheduler.err:
				logger.Fatal("receive error from scheduler", zap.Error(e))
			case <-scheduler.end:
				logger.Info("SaucrServerTestRunner.Run7 terminated!")
				return
			}
		}
	},
}

func TurnSaucrIntoVolatile() (oldSCfg *etcdserver.SaucrConfig) {
	oldSCfg = GlobalRunnerConfigs["saucr"].(*etcdserver.SaucrConfig)

	newSCfg := &etcdserver.SaucrConfig{
		MaxLocalCacheSize: oldSCfg.MaxLocalCacheSize,
		CachePreserveTime: oldSCfg.CachePreserveTime,
		HbcounterType:     adaptive.AlwaysConnectHbCounterFactory,
		SaucrModeSync:     oldSCfg.SaucrModeSync,
		SaucrModeItv:      oldSCfg.SaucrModeItv,
	}

	GlobalRunnerConfigs["saucr"] = newSCfg
	return oldSCfg
}

func TurnSaucrIntoPersistent() (oldSCfg *etcdserver.SaucrConfig) {
	oldSCfg = GlobalRunnerConfigs["saucr"].(*etcdserver.SaucrConfig)

	newSCfg := &etcdserver.SaucrConfig{
		MaxLocalCacheSize: oldSCfg.MaxLocalCacheSize,
		CachePreserveTime: oldSCfg.CachePreserveTime,
		HbcounterType:     adaptive.AlwaysDisconnectHbCounterFactory,
		SaucrModeSync:     oldSCfg.SaucrModeSync,
		SaucrModeItv:      oldSCfg.SaucrModeItv,
	}

	GlobalRunnerConfigs["saucr"] = newSCfg
	return oldSCfg
}

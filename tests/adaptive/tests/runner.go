package tests

import (
	"go.etcd.io/etcd/adaptive"
	"go.etcd.io/etcd/etcdserver"
)

type TestRunner struct {
	Run1 func(s Scheduler)
	Run3 func(s Scheduler)
	Run5 func(s Scheduler)
	Run7 func(s Scheduler)
}

var GlobalRunnerConfigs = make(map[string]interface{})

func InitRunnerConfig() {
	GlobalRunnerConfigs["c1"] = DefaultLocalCluster1
	GlobalRunnerConfigs["c3"] = DefaultLocalCluster3
	GlobalRunnerConfigs["c5"] = DefaultLocalCluster5
	GlobalRunnerConfigs["c7"] = DefaultLocalCluster7

	GlobalRunnerConfigs["saucr"] = &etcdserver.SaucrConfig{
		MaxLocalCacheSize: adaptive.DefaultStrategy.MaxLocalCacheSize,
		CachePreserveTime: adaptive.DefaultStrategy.CachePreserveTime,
	}

	GlobalRunnerConfigs["standaloneIdx"] = 0
}

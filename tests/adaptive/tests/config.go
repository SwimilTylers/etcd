package tests

import (
	"go.etcd.io/etcd/etcdserver"
	"time"
)

var GlobalRunnerConfigs = make(map[string]interface{})

func InitRunnerConfig() {
	GlobalRunnerConfigs["log-file-format"] = "etcd-%d-of-%d.log"

	GlobalRunnerConfigs["c1"] = DefaultLocalCluster1
	GlobalRunnerConfigs["c3"] = DefaultLocalCluster3
	GlobalRunnerConfigs["c5"] = DefaultLocalCluster5
	GlobalRunnerConfigs["c7"] = DefaultLocalCluster7

	GlobalRunnerConfigs["remain-duration"] = 20 * time.Second

	GlobalRunnerConfigs["saucr"] = etcdserver.DefaultSaucrConfig

	GlobalRunnerConfigs["standaloneIdx"] = 0
}

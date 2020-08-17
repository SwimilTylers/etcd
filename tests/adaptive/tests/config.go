package tests

import (
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver"
	"go.uber.org/zap"
	"time"
)

var GlobalRunnerConfigs = make(map[string]interface{})

func InitRunnerConfig() {
	GlobalRunnerConfigs["log-file-format"] = "etcd-%d-of-%d.log"
	GlobalRunnerConfigs["scheduler-log-file"] = "sch.log"

	GlobalRunnerConfigs["c1"] = DefaultLocalCluster1
	GlobalRunnerConfigs["c3"] = DefaultLocalCluster3
	GlobalRunnerConfigs["c5"] = DefaultLocalCluster5
	GlobalRunnerConfigs["c7"] = DefaultLocalCluster7

	GlobalRunnerConfigs["remain-duration"] = 20 * time.Second

	GlobalRunnerConfigs["saucr"] = etcdserver.DefaultSaucrConfig

	GlobalRunnerConfigs["standaloneIdx"] = 0

	GlobalRunnerConfigs["sch-shut"] = ShutGen(func(cluster *CDescriptor, id int) func(etcd *embed.Etcd) (*embed.Etcd, error) {
		return func(etcd *embed.Etcd) (*embed.Etcd, error) {
			etcd.Close()
			etcd.Server.Logger().Info("this server is shut down by scheduler",
				zap.String("srv-name", etcd.Server.Cfg.Name),
				zap.String("srv-id", etcd.Server.ID().String()),
			)
			return etcd, nil
		}
	})

	GlobalRunnerConfigs["sch-restart"] = RestartGen(func(cluster *CDescriptor, id int, r func(*CDescriptor, int) (*embed.Etcd, error)) func(etcd *embed.Etcd) (*embed.Etcd, error) {
		return func(etcd *embed.Etcd) (*embed.Etcd, error) {
			srv, err := r(cluster, id)
			if err != nil && srv != nil {
				srv.Server.Logger().Info("this server will restart by scheduler",
					zap.String("srv-name", srv.Server.Cfg.Name),
					zap.String("srv-id", srv.Server.ID().String()),
					zap.Int("restart-no", id),
				)
				<-srv.Server.ReadyNotify()
			}
			return srv, err
		}
	})
}

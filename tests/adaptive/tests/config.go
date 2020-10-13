package tests

import (
	"fmt"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver"
	"go.uber.org/zap"
	"strings"
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
			if err == nil && srv != nil {
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

	GlobalRunnerConfigs["sch-soft-shut"] = GlobalRunnerConfigs["sch-shut"]

	GlobalRunnerConfigs["sch-sim"] = SimGen(func(option string, descriptor *CDescriptor, id int) func(etcd *embed.Etcd) (*embed.Etcd, error) {
		sim := GlobalRunnerConfigs["tc"].(NetworkSimulator)
		logger, _ := GetSchedulerLogger()

		if strings.HasPrefix(option, "remove") {
			option = strings.Trim(strings.TrimPrefix(option, "remove"), "- ")
			if s, err := sim.FindFittestOption(option); err != nil {
				panic(fmt.Sprintf("cannot find option: %s", option))
			} else {
				return func(etcd *embed.Etcd) (*embed.Etcd, error) {
					if err := sim.UnsetByPort(descriptor.members[id].pPort.Port(), s); err != nil {
						logger.Error("failed to unset tc filter",
							zap.Error(err),
							zap.String("sim-option", s),
							zap.String("srv-name", descriptor.members[id].name),
							zap.Int("sim-no", id),
						)
						return etcd, err
					} else {
						logger.Info("unset tc filter",
							zap.String("sim-option", s),
							zap.String("srv-name", descriptor.members[id].name),
							zap.Int("sim-no", id),
						)
						return etcd, nil
					}
				}
			}
		} else {
			if s, err := sim.FindFittestOption(option); err != nil {
				panic(fmt.Sprintf("cannot find option: %s", option))
			} else {
				return func(etcd *embed.Etcd) (*embed.Etcd, error) {
					if err := sim.SetByPort(descriptor.members[id].pPort.Port(), s); err != nil {
						logger.Error("failed to set tc filter",
							zap.Error(err),
							zap.String("sim-option", s),
							zap.String("srv-name", descriptor.members[id].name),
							zap.Int("sim-no", id),
						)
						return etcd, err
					} else {
						logger.Info("set tc filter",
							zap.String("sim-option", s),
							zap.String("srv-name", descriptor.members[id].name),
							zap.Int("sim-no", id),
						)
						return etcd, nil
					}
				}
			}
		}
	})

	GlobalRunnerConfigs["rw-weight"] = StandardWeights
}

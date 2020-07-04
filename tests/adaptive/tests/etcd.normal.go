package tests

import (
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
	"log"
)

func startOneNormal(cluster *CDescriptor, idx int) (*embed.Etcd, error) {
	cfg := cluster.GetConfig(idx, embed.ClusterStateFlagNew)
	return embed.StartEtcd(cfg)
}

func restartOneNormal(cluster *CDescriptor, idx int) (*embed.Etcd, error) {
	cfg := cluster.GetConfig(idx, embed.ClusterStateFlagExisting)
	return embed.StartEtcd(cfg)
}

var NormalServerTestRunner = TestRunner{
	Start:   startOneNormal,
	Restart: restartOneNormal,
	RunX: func(selected []int, scheduler Scheduler) {
		s := make([]*embed.Etcd, len(selected))
		for i := 0; i < len(s); i++ {
			go func(idx int) {
				srv, err := startOneNormal(GlobalRunnerConfigs["cx"].(*CDescriptor), selected[idx])
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
				logger.Info("NormalServerTestRunner.Run terminated!")
				return
			}
		}
	},
	Run1: func(scheduler Scheduler) {
		srv, err := startOneNormal(GlobalRunnerConfigs["c1"].(*CDescriptor), GlobalRunnerConfigs["standaloneIdx"].(int))
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
		s := make([]*embed.Etcd, 3)
		for i := 0; i < 3; i++ {
			go func(idx int) {
				srv, err := startOneNormal(GlobalRunnerConfigs["c3"].(*CDescriptor), idx)
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
				logger.Info("NormalServerTestRunner.Run3 terminated!")
				return
			}
		}
	},
	Run5: func(scheduler Scheduler) {
		s := make([]*embed.Etcd, 5)
		for i := 0; i < 5; i++ {
			go func(idx int) {
				srv, err := startOneNormal(GlobalRunnerConfigs["c5"].(*CDescriptor), idx)
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
				logger.Info("NormalServerTestRunner.Run5 terminated!")
				return
			}
		}
	},
	Run7: func(scheduler Scheduler) {
		s := make([]*embed.Etcd, 7)
		for i := 0; i < 7; i++ {
			go func(idx int) {
				srv, err := startOneNormal(GlobalRunnerConfigs["c7"].(*CDescriptor), idx)
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
				logger.Info("NormalServerTestRunner.Run7 terminated!")
				return
			}
		}
	},
}

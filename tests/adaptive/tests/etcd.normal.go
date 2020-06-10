package tests

import (
	"go.etcd.io/etcd/embed"
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

		for {
			select {
			case e := <-scheduler.err:
				log.Fatal(e)
			case <-scheduler.end:
				log.Println("NormalServerTestRunner.Run1 terminated!")
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

		for {
			select {
			case e := <-scheduler.err:
				log.Fatal(e)
			case <-scheduler.end:
				log.Println("NormalServerTestRunner.Run3 terminated!")
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

		for {
			select {
			case e := <-scheduler.err:
				log.Fatal(e)
			case <-scheduler.end:
				log.Println("NormalServerTestRunner.Run5 terminated!")
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

		for {
			select {
			case e := <-scheduler.err:
				log.Fatal(e)
			case <-scheduler.end:
				log.Println("NormalServerTestRunner.Run7 terminated!")
				return
			}
		}
	},
}

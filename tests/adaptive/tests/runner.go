package tests

import (
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
	"log"
	"strings"
)

type TestRunner struct {
	Name    string
	Start   func(c *CDescriptor, idx int) (*embed.Etcd, error)
	Restart func(c *CDescriptor, idx int) (*embed.Etcd, error)
	RunX    func(selected []int, s Scheduler)
	Run1    func(s Scheduler)
	Run3    func(s Scheduler)
	Run5    func(s Scheduler)
	Run7    func(s Scheduler)
}

var GeneralGoroutineTestRunner = TestRunner{
	RunX: func(selected []int, scheduler Scheduler) {
		runner := GlobalRunnerConfigs["g_runner"].(TestRunner)

		s := make([]*embed.Etcd, len(selected))
		for i := 0; i < len(s); i++ {
			go func(idx int) {
				srv, err := runner.Start(GlobalRunnerConfigs["cx"].(*CDescriptor), selected[idx])
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
				logger.Info(strings.Join([]string{runner.Name, "[Goroutine version].RunX terminated!"}, ""))
				return
			}
		}
	},
	Run1: func(scheduler Scheduler) {
		runner := GlobalRunnerConfigs["g_runner"].(TestRunner)

		srv, err := runner.Start(GlobalRunnerConfigs["c1"].(*CDescriptor), GlobalRunnerConfigs["standaloneIdx"].(int))
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
				logger.Info(strings.Join([]string{runner.Name, "[Goroutine version].Run1 terminated!"}, ""))
				return
			}
		}
	},
	Run3: func(scheduler Scheduler) {
		runner := GlobalRunnerConfigs["g_runner"].(TestRunner)

		s := make([]*embed.Etcd, 3)
		for i := 0; i < 3; i++ {
			go func(idx int) {
				srv, err := runner.Start(GlobalRunnerConfigs["c3"].(*CDescriptor), idx)
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
				logger.Info(strings.Join([]string{runner.Name, "[Goroutine version].Run3 terminated!"}, ""))
				return
			}
		}
	},
	Run5: func(scheduler Scheduler) {
		runner := GlobalRunnerConfigs["g_runner"].(TestRunner)

		s := make([]*embed.Etcd, 5)
		for i := 0; i < 5; i++ {
			go func(idx int) {
				srv, err := runner.Start(GlobalRunnerConfigs["c5"].(*CDescriptor), idx)
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
				logger.Info(strings.Join([]string{runner.Name, "[Goroutine version].Run5 terminated!"}, ""))
				return
			}
		}
	},
	Run7: func(scheduler Scheduler) {
		runner := GlobalRunnerConfigs["g_runner"].(TestRunner)

		s := make([]*embed.Etcd, 7)
		for i := 0; i < 7; i++ {
			go func(idx int) {
				srv, err := runner.Start(GlobalRunnerConfigs["c7"].(*CDescriptor), idx)
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
				logger.Info(strings.Join([]string{runner.Name, "[Goroutine version].Run7 terminated!"}, ""))
				return
			}
		}
	},
}

var GeneralProcessTestRunner = TestRunner{
	RunX: func(selected []int, scheduler Scheduler) {
		runner := GlobalRunnerConfigs["g_runner"].(TestRunner)

		s := make([]*embed.Etcd, len(selected))
		for i := 0; i < len(s); i++ {
			go func(idx int) {
				srv, err := runner.Start(GlobalRunnerConfigs["cx"].(*CDescriptor), selected[idx])
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
				logger.Info(strings.Join([]string{runner.Name, "[Process version].RunX terminated!"}, ""))
				return
			}
		}
	},
	Run1: func(scheduler Scheduler) {
		runner := GlobalRunnerConfigs["g_runner"].(TestRunner)

		srv, err := runner.Start(GlobalRunnerConfigs["c1"].(*CDescriptor), GlobalRunnerConfigs["standaloneIdx"].(int))
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
				logger.Info(strings.Join([]string{runner.Name, "[Process version].Run1 terminated!"}, ""))
				return
			}
		}
	},
	Run3: func(scheduler Scheduler) {
		runner := GlobalRunnerConfigs["g_runner"].(TestRunner)

		s := make([]*embed.Etcd, 3)
		for i := 0; i < 3; i++ {
			go func(idx int) {
				srv, err := runner.Start(GlobalRunnerConfigs["c3"].(*CDescriptor), idx)
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
				logger.Info(strings.Join([]string{runner.Name, "[Process version].Run3 terminated!"}, ""))
				return
			}
		}
	},
	Run5: func(scheduler Scheduler) {
		runner := GlobalRunnerConfigs["g_runner"].(TestRunner)

		s := make([]*embed.Etcd, 5)
		for i := 0; i < 5; i++ {
			go func(idx int) {
				srv, err := runner.Start(GlobalRunnerConfigs["c5"].(*CDescriptor), idx)
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
				logger.Info(strings.Join([]string{runner.Name, "[Process version].Run5 terminated!"}, ""))
				return
			}
		}
	},
	Run7: func(scheduler Scheduler) {
		runner := GlobalRunnerConfigs["g_runner"].(TestRunner)

		s := make([]*embed.Etcd, 7)
		for i := 0; i < 7; i++ {
			go func(idx int) {
				srv, err := runner.Start(GlobalRunnerConfigs["c7"].(*CDescriptor), idx)
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
				logger.Info(strings.Join([]string{runner.Name, "[Process version].Run7 terminated!"}, ""))
				return
			}
		}
	},
}

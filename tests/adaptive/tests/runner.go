package tests

import (
	"errors"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/types"
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
		if done, ok := GlobalRunnerConfigs["slave_process"]; ok {
			//it is a slave process, init the corresponding servers
			// Scheduler of a slave process only affects its process-owned servers.
			// In avoid of confession, a doNothing slave scheduler is recommended.
			runner := GlobalRunnerConfigs["g_runner"].(TestRunner)
			logger, _ := GetSchedulerLogger()
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
					if fDone, ok := done.(func(slaveIdx int, id types.ID)); ok {
						fDone(selected[idx], srv.Server.ID())
						scheduler.do(selected[idx], srv)
					} else {
						logger.Error("scheduler init failed", zap.Error(errors.New("no proper init func")))
					}

				}(i)
			}

			defer func() {
				logger.Sync()
			}()

			for {
				select {
				case e := <-scheduler.err:
					logger.Error("receive error from scheduler", zap.Error(e))
				case <-scheduler.end:
					logger.Info(strings.Join([]string{runner.Name, "[Process version].RunX terminated!"}, ""))
					return
				}
			}
		} else {
			//it is a master process, just init slave processes
			// Scheduler of a master process affects the whole cluster with cooperation of other master schedulers.
			// Logger of master scheduler: MasterProcessKit.logger

			kit := GlobalRunnerConfigs["master-kit"].(*MasterProcessKit)
			name := GlobalRunnerConfigs["g_runner"].(TestRunner).Name

			<-kit.StartX(selected...)

			for _, i := range selected {
				go func(idx int) {
					scheduler.do(idx, nil)
				}(i)
			}

			logger := kit.logger
			for {
				select {
				case e := <-scheduler.err:
					logger.Error("receive error from scheduler", zap.Error(e))
				case <-scheduler.end:
					logger.Info(strings.Join([]string{name, "[Process version].RunX terminated!"}, ""))
					return
				}
			}
		}
	},
	Run1: func(scheduler Scheduler) {
		if _, ok := GlobalRunnerConfigs["slave_process"]; ok {
			//it is a slave process, init the corresponding servers
			// Scheduler of a slave process only affects its process-owned servers.
			// In avoid of confession, a doNothing slave scheduler is recommended.
			panic("GeneralProcessTestRunner.Run1 is not supported on slave processes")
		} else {
			//it is a master process, just init slave processes
			// Scheduler of a master process affects the whole cluster with cooperation of other master schedulers.
			// Logger of master scheduler: MasterProcessKit.logger

			kit := GlobalRunnerConfigs["master-kit"].(*MasterProcessKit)
			name := GlobalRunnerConfigs["g_runner"].(TestRunner).Name

			<-kit.StartX(0)

			go func() {
				scheduler.do(0, nil)
			}()

			logger := kit.logger
			defer logger.Sync()

			for {
				select {
				case e := <-scheduler.err:
					logger.Error("receive error from scheduler", zap.Error(e))
				case <-scheduler.end:
					logger.Info(strings.Join([]string{name, "[Process version].Run1 terminated!"}, ""))
					return
				}
			}
		}
	},
	Run3: func(scheduler Scheduler) {
		if _, ok := GlobalRunnerConfigs["slave_process"]; ok {
			//it is a slave process, init the corresponding servers
			// Scheduler of a slave process only affects its process-owned servers.
			// In avoid of confession, a doNothing slave scheduler is recommended.
			panic("GeneralProcessTestRunner.Run3 is not supported on slave processes")
		} else {
			//it is a master process, just init slave processes
			// Scheduler of a master process affects the whole cluster with cooperation of other master schedulers.
			// Logger of master scheduler: MasterProcessKit.logger

			kit := GlobalRunnerConfigs["master-kit"].(*MasterProcessKit)
			name := GlobalRunnerConfigs["g_runner"].(TestRunner).Name

			<-kit.StartX(0, 1, 2)

			for i := 0; i < 3; i++ {
				go func(idx int) {
					scheduler.do(idx, nil)
				}(i)
			}

			logger := kit.logger
			for {
				select {
				case e := <-scheduler.err:
					logger.Error("receive error from scheduler", zap.Error(e))
				case <-scheduler.end:
					logger.Info(strings.Join([]string{name, "[Process version].Run3 terminated!"}, ""))
					return
				}
			}
		}
	},
	Run5: func(scheduler Scheduler) {
		if _, ok := GlobalRunnerConfigs["slave_process"]; ok {
			//it is a slave process, init the corresponding servers
			// Scheduler of a slave process only affects its process-owned servers.
			// In avoid of confession, a doNothing slave scheduler is recommended.
			panic("GeneralProcessTestRunner.Run5 is not supported on slave processes")
		} else {
			//it is a master process, just init slave processes
			// Scheduler of a master process affects the whole cluster with cooperation of other master schedulers.
			// Logger of master scheduler: MasterProcessKit.logger

			kit := GlobalRunnerConfigs["master-kit"].(*MasterProcessKit)
			name := GlobalRunnerConfigs["g_runner"].(TestRunner).Name

			<-kit.StartX(0, 1, 2, 3, 4)

			for i := 0; i < 5; i++ {
				go func(idx int) {
					scheduler.do(idx, nil)
				}(i)
			}

			logger := kit.logger

			for {
				select {
				case e := <-scheduler.err:
					logger.Error("receive error from scheduler", zap.Error(e))
				case <-scheduler.end:
					logger.Info(strings.Join([]string{name, "[Process version].Run5 terminated!"}, ""))
					return
				}
			}
		}
	},
	Run7: func(scheduler Scheduler) {
		if _, ok := GlobalRunnerConfigs["slave_process"]; ok {
			//it is a slave process, init the corresponding servers
			// Scheduler of a slave process only affects its process-owned servers.
			// In avoid of confession, a doNothing slave scheduler is recommended.
			panic("GeneralProcessTestRunner.Run7 is not supported on slave processes")
		} else {
			//it is a master process, just init slave processes
			// Scheduler of a master process affects the whole cluster with cooperation of other master schedulers.
			// Logger of master scheduler: MasterProcessKit.logger

			kit := GlobalRunnerConfigs["master-kit"].(*MasterProcessKit)
			name := GlobalRunnerConfigs["g_runner"].(TestRunner).Name

			<-kit.StartX(0, 1, 2, 3, 4, 5, 6)

			for i := 0; i < 7; i++ {
				go func(idx int) {
					scheduler.do(idx, nil)
				}(i)
			}

			logger := kit.logger

			for {
				select {
				case e := <-scheduler.err:
					logger.Error("receive error from scheduler", zap.Error(e))
				case <-scheduler.end:
					logger.Info(strings.Join([]string{name, "[Process version].Run7 terminated!"}, ""))
					return
				}
			}
		}
	},
}

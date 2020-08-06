package tests

import (
	"errors"
	"fmt"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
	"strings"
	"time"
)

type Scheduler struct {
	do  func(int, *embed.Etcd)
	err chan error
	end chan struct{}

	desc string
}

func (s Scheduler) String() string {
	return s.desc
}

var DoNothing = Scheduler{do: func(int, *embed.Etcd) {}, err: make(chan error, 100), end: make(chan struct{}), desc: "do nothing"}

type SchedulerEvent struct {
	after time.Duration
	test  func(etcd *embed.Etcd, localMark bool) bool
	event func(etcd *embed.Etcd) (*embed.Etcd, error)
}

type SchedulerBuilder struct {
	disableCheck bool
	events       [][]*SchedulerEvent
	topIsRunning []bool

	designator CrashDesignator

	srvTerminate   chan struct{}
	totalTerminate chan struct{}

	shutFuncGen    func(cluster *CDescriptor, id int) func(etcd *embed.Etcd) (*embed.Etcd, error)
	restartFuncGen func(cluster *CDescriptor, id int, r func(*CDescriptor, int) (*embed.Etcd, error)) func(etcd *embed.Etcd) (*embed.Etcd, error)

	desc strings.Builder
}

func NewSchedulerBuilder(size int) *SchedulerBuilder {
	ret := &SchedulerBuilder{
		events:         make([][]*SchedulerEvent, size),
		topIsRunning:   make([]bool, size),
		srvTerminate:   make(chan struct{}),
		totalTerminate: make(chan struct{}),
		desc:           strings.Builder{},
	}
	for i := range ret.events {
		ret.events[i] = make([]*SchedulerEvent, 0, 10)
	}
	return ret
}

func (b *SchedulerBuilder) Init() *SchedulerBuilder {
	for i := 0; i < len(b.topIsRunning); i++ {
		b.topIsRunning[i] = true
	}
	b.disableCheck = false
	b.desc.WriteString(fmt.Sprintf("[init, size=%d]", len(b.topIsRunning)))

	b.shutFuncGen = GlobalRunnerConfigs["sch-shut"].(func(cluster *CDescriptor, id int) func(etcd *embed.Etcd) (*embed.Etcd, error))
	b.restartFuncGen = GlobalRunnerConfigs["sch-restart"].(func(cluster *CDescriptor, id int, r func(*CDescriptor, int) (*embed.Etcd, error)) func(etcd *embed.Etcd) (*embed.Etcd, error))

	return b
}

func (b *SchedulerBuilder) DisableStaticRunningStateCheck() *SchedulerBuilder {
	b.disableCheck = true
	return b
}

func (b *SchedulerBuilder) LoadDesignator(designator CrashDesignator) *SchedulerBuilder {
	b.designator = designator
	return b
}

func (b *SchedulerBuilder) IdleAll(after time.Duration) *SchedulerBuilder {
	idle := make([]bool, len(b.events))
	for i := range idle {
		idle[i] = true
	}

	b.idleInternal(after, idle)
	b.desc.WriteString(fmt.Sprintf("=%v=>[idle,all]", after))
	return b
}

func (b *SchedulerBuilder) ChangeCondition(srv []int, condition ConditionDesc) *SchedulerBuilder {
	if !b.disableCheck {
		panic("condition-related ops is permitted when SRS Check is disabled")
	}

	for _, s := range srv {
		b.events[s] = append(b.events[s],
			&SchedulerEvent{
				after: 0,
				test:  condition.test,
				event: func(etcd *embed.Etcd) (*embed.Etcd, error) { return etcd, nil }, // in avoid of idle-merging
			},
		)
	}
	b.desc.WriteString(fmt.Sprintf("=>[set,%v:=%s]", srv, condition.desc))
	return b
}

func (b *SchedulerBuilder) ChangeConditionAll(condition ConditionDesc) *SchedulerBuilder {
	if !b.disableCheck {
		panic("condition-related ops is permitted when SRS Check is disabled")
	}

	for i, e := range b.events {
		b.events[i] = append(e,
			&SchedulerEvent{
				after: 0,
				test:  condition.test,
				event: func(etcd *embed.Etcd) (*embed.Etcd, error) { return etcd, nil }, // in avoid of idle-merging
			},
		)
	}
	b.desc.WriteString(fmt.Sprintf("=>[set,all:=%s]", condition.desc))
	return b
}

func (b *SchedulerBuilder) Shutdown(after time.Duration, srv []int) *SchedulerBuilder {
	idle := make([]bool, len(b.events))
	for i := range idle {
		idle[i] = true
	}

	cluster := GlobalRunnerConfigs[fmt.Sprintf("c%d", len(b.events))].(*CDescriptor)

	for _, s := range srv {
		idle[s] = false
		b.events[s] = append(b.events[s],
			&SchedulerEvent{
				after: after,
				test:  ConditionTrue.test,
				event: b.shutFuncGen(cluster, s),
			},
		)
		b.topIsRunning[s] = false
	}

	b.idleInternal(after, idle)
	b.desc.WriteString(fmt.Sprintf("=%v=>[shut,%v]", after, srv))
	return b
}

func (b *SchedulerBuilder) ShutdownNext(after time.Duration) *SchedulerBuilder {
	return b.Shutdown(after, b.designator.NextCrash(1))
}

func (b *SchedulerBuilder) ShutdownOnCondition(after time.Duration, condition ConditionDesc) *SchedulerBuilder {
	if !b.disableCheck {
		panic("condition-related ops is permitted when SRS Check is disabled")
	}

	cluster := GlobalRunnerConfigs[fmt.Sprintf("c%d", len(b.events))].(*CDescriptor)

	for i, e := range b.events {
		b.events[i] = append(e,
			&SchedulerEvent{
				after: after,
				test:  condition.test,
				event: b.shutFuncGen(cluster, i),
			},
		)
	}
	b.desc.WriteString(fmt.Sprintf("=%v=>[shut,%s]", after, condition.desc))
	return b
}

func (b *SchedulerBuilder) ShutdownAll(after time.Duration) *SchedulerBuilder {
	cluster := GlobalRunnerConfigs[fmt.Sprintf("c%d", len(b.events))].(*CDescriptor)

	for i, e := range b.events {
		b.events[i] = append(e,
			&SchedulerEvent{
				after: after,
				test:  ConditionTrue.test,
				event: b.shutFuncGen(cluster, i),
			},
		)
		b.topIsRunning[i] = false
	}
	b.desc.WriteString(fmt.Sprintf("=%v=>[shut,all]", after))
	return b
}

func (b *SchedulerBuilder) Restart(after time.Duration, r func(*CDescriptor, int) (*embed.Etcd, error), srv []int) *SchedulerBuilder {
	idle := make([]bool, len(b.events))
	for i := range idle {
		idle[i] = true
	}

	cluster := GlobalRunnerConfigs[fmt.Sprintf("c%d", len(b.events))].(*CDescriptor)
	for _, s := range srv {
		if b.topIsRunning[s] && !b.disableCheck {
			panic("[SRS Check]: cannot restart a running srv")
		}
		idle[s] = false
		b.events[s] = append(b.events[s],
			&SchedulerEvent{
				after: after,
				test:  ConditionTrue.test,
				event: b.restartFuncGen(cluster, s, r),
			},
		)
		b.topIsRunning[s] = true
	}
	b.desc.WriteString(fmt.Sprintf("=%v=>[restart,%v]", after, srv))
	b.idleInternal(after, idle)

	return b
}

func (b *SchedulerBuilder) RestartNext(after time.Duration, r func(*CDescriptor, int) (*embed.Etcd, error)) *SchedulerBuilder {
	return b.Restart(after, r, b.designator.NextRestart(1))
}

func (b *SchedulerBuilder) RestartOnCondition(after time.Duration, r func(*CDescriptor, int) (*embed.Etcd, error), condition ConditionDesc) *SchedulerBuilder {
	if !b.disableCheck {
		panic("condition-related ops is permitted when SRS Check is disabled")
	}

	idle := make([]bool, len(b.events))
	for i := range idle {
		idle[i] = true
	}

	cluster := GlobalRunnerConfigs[fmt.Sprintf("c%d", len(b.events))].(*CDescriptor)
	for s, e := range b.events {
		idle[s] = false
		b.events[s] = append(e,
			&SchedulerEvent{
				after: after,
				test:  condition.test,
				event: b.restartFuncGen(cluster, s, r),
			},
		)
	}

	b.idleInternal(after, idle)
	b.desc.WriteString(fmt.Sprintf("=%v=>[restart,all]", after))
	return b
}

func (b *SchedulerBuilder) daemon() {
	var count = 0
	for s := range b.srvTerminate {
		count++
		if count == len(b.events) {
			b.totalTerminate <- s
			return
		}
	}
}

func (b *SchedulerBuilder) AutoBuild(itv time.Duration, r func(*CDescriptor, int) (*embed.Etcd, error)) Scheduler {
	next, srv := b.designator.NextAction()
	for next != CDActionStop {
		switch next {
		case CDActionNop:
			b.IdleAll(itv)
		case CDActionCrash:
			b.Shutdown(itv, srv)
		case CDActionRestart:
			b.Restart(itv, r, srv)
		}
		next, srv = b.designator.NextAction()
	}
	return b.Build()
}

func (b *SchedulerBuilder) Build() Scheduler {
	remain := GlobalRunnerConfigs["remain-duration"].(time.Duration)
	b.desc.WriteString(fmt.Sprintf("=%v=>[stop]", remain))

	sch := Scheduler{
		err:  make(chan error, len(b.events)),
		end:  b.totalTerminate,
		desc: b.desc.String(),
	}

	sch.do = func(i int, etcd *embed.Etcd) {
		event := b.events[i]
		var mark = true
		for _, schedulerEvent := range event {
			<-time.After(schedulerEvent.after)

			mark = schedulerEvent.test(etcd, mark)

			if schedulerEvent.event != nil && mark {
				var err error
				if etcd, err = schedulerEvent.event(etcd); err != nil {
					sch.err <- errors.New(fmt.Sprintf("from [%d]: %v", i, err))
				}
			}
		}
		<-time.After(remain)
		b.srvTerminate <- struct{}{}
		etcd.Server.Logger().Info("this server is shut down by scheduler",
			zap.String("srv-name", etcd.Server.Cfg.Name),
			zap.String("srv-id", etcd.Server.ID().String()),
		)
	}

	go b.daemon()

	return sch
}

func (b *SchedulerBuilder) idleInternal(after time.Duration, shouldIde []bool) {
	for i, idle := range shouldIde {
		if idle {
			last := len(b.events[i]) - 1
			if len(b.events[i]) == 0 || b.events[i][last].event != nil {
				b.events[i] = append(b.events[i], &SchedulerEvent{
					after: after,
					test:  ConditionSame.test,
					event: nil,
				})
			} else {
				b.events[i][last].after += after
			}
		}
	}
}

func (b *SchedulerBuilder) syncInternal(after time.Duration, syncC <-chan struct{}, count int, shouldSync []bool) {
	panic("not support yet")
}

type ConditionDesc struct {
	test func(etcd *embed.Etcd, localMark bool) bool
	desc string
}

var (
	ConditionTrue     = ConditionDesc{func(etcd *embed.Etcd, localMark bool) bool { return true }, "all"}
	ConditionFalse    = ConditionDesc{func(etcd *embed.Etcd, localMark bool) bool { return false }, "none"}
	ConditionSame     = ConditionDesc{func(etcd *embed.Etcd, localMark bool) bool { return localMark }, "still"}
	ConditionReverse  = ConditionDesc{func(etcd *embed.Etcd, localMark bool) bool { return !localMark }, "reverse"}
	ConditionIsLeader = ConditionDesc{func(etcd *embed.Etcd, localMark bool) bool { return etcd.Server.ID() == etcd.Server.Leader() }, "leader"}
)

func GetSchedulerLogger() (*zap.Logger, error) {
	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	config.ErrorOutputPaths = append(config.ErrorOutputPaths, GlobalRunnerConfigs["scheduler-log-file"].(string))
	config.OutputPaths = append(config.OutputPaths, GlobalRunnerConfigs["scheduler-log-file"].(string))

	return config.Build()
}

/*
func GetTimeoutScheduler(d time.Duration) Scheduler {
	return func(idx int, e *embed.Etcd) {
		select {
		case <-e.Server.ReadyNotify():
			log.Printf("Server is ready!")
		case <-time.After(d):
			e.Server.Stop() // trigger a shutdown
			log.Printf("Server took too long to start!")
		}
		log.Fatal(<-e.Err())
	}
}
*/

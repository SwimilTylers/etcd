package main

import (
	"bufio"
	"flag"
	"fmt"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/tests/adaptive/tests"
	"go.etcd.io/etcd/tests/adaptive/utils"
	"math/rand"
	"os"
	"strconv"
	"time"
)

var (
	restart   = flag.Bool("restart", false, "whether running in restart mode")
	scheduler = flag.String("s", "mode switch", "choose scheduler type")
	bench     = flag.String("b", "benchtool", "choose bench tool")
	runner    = flag.String("r", "etcd", "choose runner type")
	pGenType  = flag.String("pg", "sequential", "choose designator perm generator type")
	pGenSeed  = flag.Int64("seed", rand.Int63(), "give random walk designator a seed. Only works when choosing 'sync walk' or 'async walk' designator perm generator")
	violent   = flag.Bool("v", false, "whether to terminate server violently when necessary. If false, use etcd internal signal mechanism")

	expGIns = flag.Bool("experimental-goroutine-instances", false, "use experimental GeneralGoroutineTesterRunner as the wrapper runner")
	expPIns = flag.Bool("experimental-process-instances", false, "use experimental GeneralProcessTesterRunner as the wrapper runner")

	role = flag.String("role", "master", "role of this process. If role sets to 'master', it runs with a functional scheduler")

	selectedS = flag.String("selected", "all", "select running servers")
	pSize     = flag.Int("p", 5, "size of server cluster")
)

func main() {
	flag.Parse()
	fmt.Println("[*] pid:", os.Getpid(), "ppid:", os.Getppid())
	if *role == "master" {
		sub := tests.ForkExecExperiment(utils.FetchAndGenerateExecArgs("-seed", strconv.FormatInt(*pGenSeed, 10), "-role", "slave"))
		tests.MasterExperiment(sub)
	} else {
		tests.SlaveExperiment()
	}
	os.Exit(0)

	tests.InitRunnerConfig()
	utils.InitClientConfig()

	// change Global Args
	tests.GlobalRunnerConfigs["remain-duration"] = 10 * time.Minute
	tests.GlobalRunnerConfigs["violent-stop"] = *violent

	// clusters
	tests.GlobalRunnerConfigs["available-machine"] = []string{
		"http://192.168.198.139",
		"http://192.168.198.138",
		"http://192.168.198.137",
	}
	tests.GlobalRunnerConfigs["local-machine"] = "http://192.168.198.137"

	if !*restart {
		if err := RemoveHistory(); err != nil {
			panic("err occurs when remove history")
		}
	}

	var size = *pSize
	// var hosts []string
	var selected = utils.ReadSelected(*selectedS)
	// tests.GlobalRunnerConfigs[fmt.Sprintf("c%d", size)] = tests.MakeUniformCluster(size, "http://192.168.198.137")
	// hosts, selected = GetRemoteCluster(size)
	// tests.GlobalRunnerConfigs[fmt.Sprintf("c%d", size)] = tests.MakeDistinctCluster(hosts)

	switch *pGenType {
	case "sequential":
		tests.GlobalRunnerConfigs["s_designator"] = tests.NewDesignator(size, tests.SequentialDesignatorPermGenerator)
	case "random":
		tests.GlobalRunnerConfigs["s_designator"] = tests.NewDesignator(size, tests.GetRandomDesignatorPermGenerator(rand.NewSource(*pGenSeed)))
	case "sync walk":
		tests.GlobalRunnerConfigs["s_designator"] = tests.NewRandomWalkDesignator(size, false, rand.NewSource(*pGenSeed))
	case "async walk":
		tests.GlobalRunnerConfigs["s_designator"] = tests.NewRandomWalkDesignator(size, true, rand.NewSource(*pGenSeed))
	default:
		panic("unknown designator perm generator type")
	}

	var tester tests.TestRunner

	switch *runner {
	case "etcd":
		tester = tests.NormalServerTestRunner
	case "saucr":
		tester = tests.SaucrServerTestRunner
	case "volatile":
		tests.TurnSaucrIntoVolatile()
		tester = tests.SaucrServerTestRunner
	case "persistent":
		tests.TurnSaucrIntoPersistent()
		tester = tests.SaucrServerTestRunner
	default:
		panic("unknown runner type")
	}

	if *expGIns {
		tester = tests.UseExperimentalTesterRunner(tester, "goroutine-based")

		if *expPIns {
			panic("conflict experimental option")
		}
	} else if *expPIns {
		tester = tests.UseExperimentalTesterRunner(tester, "process-based")

		if *expGIns {
			panic("conflict experimental option")
		}
	}

	var (
		ModeSwitchItv  = 10 * time.Second
		UnavailableItv = 10 * time.Second
		ExtremeItv     = 5 * time.Second
		AutoItv        = 5 * time.Second
	)

	var sch tests.Scheduler

	if *restart {
		sch = tests.DoNothing
	} else {
		switch *scheduler {
		case "do nothing":
			sch = tests.DoNothing
		case "leader crash":
			sch = MakeLeaderCrashScenario(tester.Restart, size, ModeSwitchItv)
		case "mode switch":
			sch = MakeModeSwitchScenario(tester.Restart, size, ModeSwitchItv)
		case "unavailable":
			sch = MakeUnavailableScenario(tester.Restart, size, UnavailableItv)
		case "mode switch + leader crash":
			sch = MakeLeaderCrashModeSwitchScenario(tester.Restart, size, ModeSwitchItv)
		case "unavailable + leader crash":
			sch = MakeLeaderCrashUnavailableScenario(tester.Restart, size, UnavailableItv)
		case "extreme":
			sch = MakeExtremeScenario(tester.Restart, size, ExtremeItv)
		case "auto":
			sch = MakeScenarios(tester.Restart, size, AutoItv)
		default:
			panic("unknown scheduler type")
		}
	}

	if *bench == "benchtool" {
		utils.UseBenchTool()
	}

	benchArgs := utils.ExtractArgs(utils.GlobalClientConfig["bench-arg-format"].(string), "put")
	benchArgs[1]["total"] = "800000"
	benchArgs[0]["clients"] = "24"
	// benchArgs[1]["wait"] = "60m"
	// benchArgs[0]["lifetime"] = "10m"
	utils.GlobalClientConfig["bench-arg-format"] = utils.MakeArgs(benchArgs, "put")

	switch *bench {
	case "benchmark":
		go utils.CreateBenchShell(size)
	case "benchtool":
		go utils.CreateBenchVerifyShell(size)
	default:
		panic("unknown bench tool")
	}

	Pause(GetTesterRunnerInfo(tester, size, selected), "\nscenario: ", sch.String())

	Run(tester, size, selected...)(sch)
}

func MakeLeaderCrashScenario(restart func(*tests.CDescriptor, int) (*embed.Etcd, error), size int, itv time.Duration) tests.Scheduler {
	switch size {
	case 3:
		return tests.NewSchedulerBuilder(size).
			Init().
			DisableStaticRunningStateCheck().
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			RestartOnCondition(itv, restart, tests.ConditionSame).
			ChangeConditionAll(tests.ConditionTrue).
			Build()
	case 5:
		return tests.NewSchedulerBuilder(size).
			Init().
			DisableStaticRunningStateCheck().
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			RestartOnCondition(itv, restart, tests.ConditionSame).
			ChangeConditionAll(tests.ConditionTrue).
			Build()
	case 7:
		return tests.NewSchedulerBuilder(size).
			Init().
			DisableStaticRunningStateCheck().
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			RestartOnCondition(itv, restart, tests.ConditionSame).
			ChangeConditionAll(tests.ConditionTrue).
			Build()
	default:
		return tests.DoNothing
	}
}

func MakeModeSwitchScenario(restart func(*tests.CDescriptor, int) (*embed.Etcd, error), size int, itv time.Duration) tests.Scheduler {
	designator := tests.GlobalRunnerConfigs["s_designator"].(tests.CrashDesignator)

	switch size {
	case 3:
		return tests.NewSchedulerBuilder(size).
			Init().
			LoadDesignator(designator).
			ShutdownNext(itv).
			RestartNext(itv, restart).
			Build()
	case 5:
		return tests.NewSchedulerBuilder(size).
			Init().
			LoadDesignator(designator).
			ShutdownNext(itv).
			ShutdownNext(itv).
			RestartNext(itv, restart).
			RestartNext(itv, restart).
			Build()
	case 7:
		return tests.NewSchedulerBuilder(size).
			Init().
			LoadDesignator(designator).
			ShutdownNext(itv).
			ShutdownNext(itv).
			ShutdownNext(itv).
			RestartNext(itv, restart).
			RestartNext(itv, restart).
			RestartNext(itv, restart).
			Build()
	default:
		return tests.DoNothing
	}
}

func MakeLeaderCrashModeSwitchScenario(restart func(*tests.CDescriptor, int) (*embed.Etcd, error), size int, itv time.Duration) tests.Scheduler {
	designator := tests.GlobalRunnerConfigs["s_designator"].(tests.CrashDesignator)

	switch size {
	case 3:
		return tests.NewSchedulerBuilder(size).
			Init().
			DisableStaticRunningStateCheck().
			LoadDesignator(designator).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			RestartOnCondition(itv, restart, tests.ConditionSame).
			ChangeConditionAll(tests.ConditionTrue).
			Build()
	case 5:
		return tests.NewSchedulerBuilder(size).
			Init().
			DisableStaticRunningStateCheck().
			LoadDesignator(designator).
			ShutdownNext(itv).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			RestartOnCondition(itv, restart, tests.ConditionSame).
			ChangeConditionAll(tests.ConditionTrue).
			RestartNext(itv, restart).
			Build()
	case 7:
		return tests.NewSchedulerBuilder(size).
			Init().
			DisableStaticRunningStateCheck().
			LoadDesignator(designator).
			ShutdownNext(itv).
			ShutdownNext(itv).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			RestartOnCondition(itv, restart, tests.ConditionSame).
			ChangeConditionAll(tests.ConditionTrue).
			RestartNext(itv, restart).
			RestartNext(itv, restart).
			Build()
	default:
		return tests.DoNothing
	}
}

func MakeUnavailableScenario(restart func(*tests.CDescriptor, int) (*embed.Etcd, error), size int, itv time.Duration) tests.Scheduler {
	designator := tests.GlobalRunnerConfigs["s_designator"].(tests.CrashDesignator)

	switch size {
	case 3:
		return tests.NewSchedulerBuilder(size).
			Init().
			LoadDesignator(designator).
			ShutdownNext(itv).
			ShutdownNext(itv).
			RestartNext(itv, restart).
			RestartNext(itv, restart).
			Build()
	case 5:
		return tests.NewSchedulerBuilder(size).
			Init().
			LoadDesignator(designator).
			ShutdownNext(itv).
			ShutdownNext(itv).
			ShutdownNext(itv).
			RestartNext(itv, restart).
			RestartNext(itv, restart).
			RestartNext(itv, restart).
			Build()
	case 7:
		return tests.NewSchedulerBuilder(size).
			Init().
			LoadDesignator(designator).
			ShutdownNext(itv).
			ShutdownNext(itv).
			ShutdownNext(itv).
			ShutdownNext(itv).
			RestartNext(itv, restart).
			RestartNext(itv, restart).
			RestartNext(itv, restart).
			RestartNext(itv, restart).
			Build()
	default:
		return tests.DoNothing
	}
}

func MakeLeaderCrashUnavailableScenario(restart func(*tests.CDescriptor, int) (*embed.Etcd, error), size int, itv time.Duration) tests.Scheduler {
	designator := tests.GlobalRunnerConfigs["s_designator"].(tests.CrashDesignator)

	switch size {
	case 3:
		return tests.NewSchedulerBuilder(size).
			Init().
			DisableStaticRunningStateCheck().
			LoadDesignator(designator).
			ShutdownNext(itv).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			RestartOnCondition(itv, restart, tests.ConditionSame).
			ChangeConditionAll(tests.ConditionTrue).
			RestartNext(itv, restart).
			Build()
	case 5:
		return tests.NewSchedulerBuilder(size).
			Init().
			DisableStaticRunningStateCheck().
			LoadDesignator(designator).
			ShutdownNext(itv).
			ShutdownNext(itv).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			RestartOnCondition(itv, restart, tests.ConditionSame).
			ChangeConditionAll(tests.ConditionTrue).
			RestartNext(itv, restart).
			RestartNext(itv, restart).
			Build()
	case 7:
		return tests.NewSchedulerBuilder(size).
			Init().
			DisableStaticRunningStateCheck().
			LoadDesignator(designator).
			ShutdownNext(itv).
			ShutdownNext(itv).
			ShutdownNext(itv).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			RestartOnCondition(itv, restart, tests.ConditionSame).
			ChangeConditionAll(tests.ConditionTrue).
			RestartNext(itv, restart).
			RestartNext(itv, restart).
			RestartNext(itv, restart).
			Build()
	default:
		return tests.DoNothing
	}
}

// the action of this scenario is unstable
func MakeExtremeScenario(restart func(*tests.CDescriptor, int) (*embed.Etcd, error), size int, itv time.Duration) tests.Scheduler {
	switch size {
	case 3:
		return tests.NewSchedulerBuilder(size).
			Init().
			DisableStaticRunningStateCheck().
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			RestartOnCondition(itv, restart, tests.ConditionSame).
			Build()
	case 5:
		return tests.NewSchedulerBuilder(size).
			Init().
			DisableStaticRunningStateCheck().
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			RestartOnCondition(itv, restart, tests.ConditionSame).
			Build()
	case 7:
		return tests.NewSchedulerBuilder(size).
			Init().
			DisableStaticRunningStateCheck().
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			ShutdownOnCondition(itv, tests.ConditionIsLeader).
			RestartOnCondition(itv, restart, tests.ConditionSame).
			Build()
	default:
		return tests.DoNothing
	}
}

func MakeScenarios(restart func(*tests.CDescriptor, int) (*embed.Etcd, error), size int, itv time.Duration) tests.Scheduler {
	designator := tests.GlobalRunnerConfigs["s_designator"].(tests.CrashDesignator)

	return tests.NewSchedulerBuilder(size).
		Init().
		LoadDesignator(designator).
		AutoBuild(itv, restart)
}

func RemoveHistory() error {
	if err := utils.RemoveAllSrvInfo(); err != nil {
		fmt.Println("cannot remove all srv info: ", err)
		return err
	} else {
		fmt.Println("all past srv info has been removed")
	}

	if err := utils.RemoveAllSrvLog(); err != nil {
		fmt.Println("cannot remove all srv log: ", err)
		return err
	} else {
		fmt.Println("all past srv log has been removed")
	}

	return nil
}

func Run(runner tests.TestRunner, size int, selected ...int) func(scheduler tests.Scheduler) {
	if selected == nil || len(selected) == 0 {
		switch size {
		case 1:
			return runner.Run1
		case 3:
			return runner.Run3
		case 5:
			return runner.Run5
		case 7:
			return runner.Run7
		default:
			return nil
		}
	} else {
		if config, ok := tests.GlobalRunnerConfigs["cx"]; !ok {
			tests.GlobalRunnerConfigs["cx"] = tests.GlobalRunnerConfigs[fmt.Sprintf("c%d", size)]
		} else if config.(*tests.CDescriptor).GetMemberNum() != size {
			return nil
		}
		return func(s tests.Scheduler) { runner.RunX(selected, s) }
	}
}

func Pause(msg ...string) {
	for _, m := range msg {
		_, _ = os.Stdout.WriteString(m)
	}
	_, _ = bufio.NewReader(os.Stdin).ReadByte()
}

func GetRemoteCluster(size int) ([]string, []int) {
	hosts := make([]string, size)
	var selected []int
	machine := tests.GlobalRunnerConfigs["available-machine"].([]string)
	local := tests.GlobalRunnerConfigs["local-machine"].(string)
	mLen := len(machine)
	for mId := 0; mId < mLen; mId++ {
		for sId := mId; sId < size; sId += mLen {
			hosts[sId] = machine[mId]
			if hosts[sId] == local {
				selected = append(selected, sId)
			}
		}
	}
	return hosts, selected
}

func GetTesterRunnerInfo(tester tests.TestRunner, size int, selected []int) string {
	if selected != nil {
		return fmt.Sprintf("ready for server instantiation [tester=%s,size=%d,selected=%v]", tester.Name, size, selected)
	} else {
		return fmt.Sprintf("ready for server instantiation [tester=%s,size=%d,selected=all]", tester.Name, size)
	}
}

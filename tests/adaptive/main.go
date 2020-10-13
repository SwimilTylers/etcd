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
	"strings"
	"time"
)

var (
	restart   = flag.Bool("restart", false, "whether running in restart mode")
	scheduler = flag.String("s", "mode switch", "choose scheduler type")
	bench     = flag.String("b", "benchtool", "choose bench tool. Only works when '-csh' sets to true")
	runner    = flag.String("r", "etcd", "choose runner type")
	pGenType  = flag.String("pg", "sequential", "choose designator perm generator type")
	pGenSeed  = flag.Int64("seed", 0, "give randomized designator a seed. Generate a random seed when it sets to zero")
	cShell    = flag.Bool("csh", true, "choose whether to generate client shell")
	rmLog     = flag.Bool("rlog", true, "choose whether to remove all log files")
	verbose   = flag.Bool("V", true, "choose whether to check test info interactively before starting")

	expGIns = flag.Bool("experimental-goroutine-instances", false, "use experimental GeneralGoroutineTesterRunner as the wrapper runner")
	expPIns = flag.Bool("experimental-process-instances", false, "use experimental GeneralProcessTesterRunner as the wrapper runner")

	role    = flag.String("role", "master", "experimental option: role of this process. Manual configuration of this flag is not recommended")
	schPort = flag.Int("master-sch-port", 3953, "experimental option: the rpc port of scheduler on master process")
	etcdctl = flag.String("using-etcdctl-path", "", "experimental option: using etcdctl and its path. If you not want to use etcdctl, please do not this option")

	violent = flag.Bool("violent", false, "experiment option: whether to terminate server violently when necessary. If false, use graceful shutdown mechanism")

	selectedS = flag.String("selected", "all", "select running servers")
	pSize     = flag.Int("p", 5, "size of server cluster")

	scItv = flag.Duration("scenario-interval", 5*time.Second, "interval between scenario events")
	rmDur = flag.Duration("remain-duration", 5*time.Minute, "remain duration after all scenario events have been done")

	rType = flag.String("rtype", "static", "type of client request number")
	rSize = flag.Int("rsize", 800000, "client request number")
	rRate = flag.Int("rrate", 0, "client request rate")
	rLife = flag.Duration("rlife", 5*time.Minute, "max duration of total request procedure")

	cSize = flag.Int("c", 24, "number of client")

	tcCfg = flag.String("tc", "", "path of tc config file")
)

func main() {
	flag.Parse()

	tests.InitRunnerConfig()
	utils.InitClientConfig()

	// change Global Args
	tests.GlobalRunnerConfigs["remain-duration"] = *rmDur
	if *pGenSeed == 0 {
		*pGenSeed = rand.Int63()
	}
	if *tcCfg != "" {
		if tc, err := utils.GetTCFromYaml(*tcCfg); err != nil {
			panic("cannot unmarshal yaml")
		} else {
			tests.GlobalRunnerConfigs["tc"] = tc
			tc.Init()
			defer tc.Clear()
		}
	}

	// clusters
	tests.GlobalRunnerConfigs["available-machine"] = []string{
		"http://192.168.198.139",
		"http://192.168.198.138",
		"http://192.168.198.137",
	}
	tests.GlobalRunnerConfigs["local-machine"] = "http://192.168.198.137"

	if *rmLog && !*restart && *role == "master" {
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
		tests.GlobalRunnerConfigs["s_designator"] = tests.NewBitonicDesignator(size, tests.SequentialDesignatorPermGenerator)
	case "random":
		tests.GlobalRunnerConfigs["designator_random_seed"] = *pGenSeed
		tests.GlobalRunnerConfigs["s_designator"] = tests.NewBitonicDesignator(size, tests.GetRandomDesignatorPermGenerator(rand.NewSource(*pGenSeed)))
	case "sync walk":
		tests.GlobalRunnerConfigs["designator_random_seed"] = *pGenSeed
		tests.GlobalRunnerConfigs["rw-weight"] = tests.FlipMoreWeights
		tests.GlobalRunnerConfigs["s_designator"] = tests.NewRandomWalkDesignator(size, false, true, rand.NewSource(*pGenSeed))
	case "async walk":
		tests.GlobalRunnerConfigs["designator_random_seed"] = *pGenSeed
		tests.GlobalRunnerConfigs["rw-weight"] = tests.FlipMoreWeights
		tests.GlobalRunnerConfigs["s_designator"] = tests.NewRandomWalkDesignator(size, true, true, rand.NewSource(*pGenSeed))
	case "flip-free sync walk":
		tests.GlobalRunnerConfigs["designator_random_seed"] = *pGenSeed
		tests.GlobalRunnerConfigs["s_designator"] = tests.NewRandomWalkDesignator(size, false, false, rand.NewSource(*pGenSeed))
	case "flip-free async walk":
		tests.GlobalRunnerConfigs["designator_random_seed"] = *pGenSeed
		tests.GlobalRunnerConfigs["s_designator"] = tests.NewRandomWalkDesignator(size, true, false, rand.NewSource(*pGenSeed))
	case "bareback":
		tests.GlobalRunnerConfigs["s_designator"] = tests.NewReshuffleDesignator(size, tests.SequentialDesignatorPermGenerator)
	case "reshuffle":
		tests.GlobalRunnerConfigs["designator_random_seed"] = *pGenSeed
		tests.GlobalRunnerConfigs["s_designator"] = tests.NewReshuffleDesignator(size, tests.GetRandomDesignatorPermGenerator(rand.NewSource(*pGenSeed)))
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
		if *role != "master" {
			panic("-role: unsupported for experimental goroutine-based tester runner option")
		}
		if *violent {
			tests.GlobalRunnerConfigs["violent-stop"] = struct{}{}
		}
	} else if *expPIns {
		tester = tests.UseExperimentalTesterRunner(tester, "process-based")
		if *expGIns {
			panic("conflict experimental option")
		}
		if *violent {
			panic("-violent: unsupported for experimental process-based tester runner option")
		}
		if *etcdctl != "" {
			tests.GlobalRunnerConfigs["etcdctl"] = *etcdctl
		}

		switch *role {
		case "master":
			tests.WorkInMasterRole(size, *pGenSeed, *schPort)
		case "slave":
			tests.WorkInSlaveRole(selected[0], *schPort, *restart)
		default:
			panic("unsupported process role")
		}
	} else {
		if *role != "master" {
			panic("-role: unsupported for non-experimental option")
		}
		if *violent {
			panic("-violent: unsupported for non-experimental option")
		}
	}

	var sch tests.Scheduler

	if *restart {
		sch = tests.DoNothing
	} else {
		switch *scheduler {
		case "do nothing":
			sch = tests.DoNothing
		case "leader crash":
			sch = MakeLeaderCrashScenario(tester.Restart, size, *scItv)
		case "mode switch":
			sch = MakeModeSwitchScenario(tester.Restart, size, *scItv)
		case "unavailable":
			sch = MakeUnavailableScenario(tester.Restart, size, *scItv)
		case "mode switch + leader crash":
			sch = MakeLeaderCrashModeSwitchScenario(tester.Restart, size, *scItv)
		case "unavailable + leader crash":
			sch = MakeLeaderCrashUnavailableScenario(tester.Restart, size, *scItv)
		case "extreme":
			sch = MakeExtremeScenario(tester.Restart, size, *scItv)
		case "data loss":
			sch = MakeDataLossBScenario(tester.Restart, size, *scItv)
		case "weak data loss":
			sch = MakeWeakDataLossBScenario(tester.Restart, size, *scItv)
		case "delay":
			sch = MakeDelayBScenario(tester.Restart, size, *scItv)
		case "auto":
			sch = MakeScenarios(tester.Restart, size, *scItv)
		default:
			panic("unknown scheduler type")
		}
	}

	if *cShell && *role == "master" {
		if *bench == "benchtool" {
			utils.UseBenchTool()
		}

		benchArgs := utils.ExtractArgs(utils.GlobalClientConfig["bench-arg-format"].(string), "put")

		switch *rType {
		case "static":
			benchArgs[1]["total"] = strconv.Itoa(*rSize)
		case "dynamic":
			steps := strings.Count(sch.String(), "=>") + 1
			benchArgs[1]["total"] = strconv.Itoa(steps * int(*scItv/time.Second) * *rRate)
		}

		benchArgs[1]["rate"] = strconv.Itoa(*rRate)
		benchArgs[0]["clients"] = strconv.Itoa(*cSize)

		switch *bench {
		case "benchmark":
			benchArgs[1]["wait"] = (*rLife).String()
		case "benchtool":
			benchArgs[0]["lifetime"] = (*rLife).String()
		}

		utils.GlobalClientConfig["bench-arg-format"] = utils.MakeArgs(benchArgs, "put")

		switch *bench {
		case "benchmark":
			go utils.CreateBenchShell(size)
		case "benchtool":
			go utils.CreateBenchVerifyShell(size)
		default:
			panic("unknown bench tool")
		}
	}

	if *verbose {
		Pause(GetTesterRunnerInfo(tester, size, selected), "\nscenario: ", sch.String())
	}

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

func MakeDataLossBScenario(restart func(*tests.CDescriptor, int) (*embed.Etcd, error), size int, itv time.Duration) tests.Scheduler {
	designator := tests.GlobalRunnerConfigs["s_designator"].(*tests.ReshuffledDesignator)

	switch size {
	case 3:
		return tests.NewSchedulerBuilder(size).
			Init().
			Shutdown(itv, designator.MapTo(0)).
			Shutdown(itv, designator.MapTo(1)).
			ShutRestart(itv, restart, designator.MapTo(2), designator.MapTo(0, 1)).
			Restart(itv, restart, designator.MapTo(2)).
			Build()
	case 5:
		return tests.NewSchedulerBuilder(size).
			Init().
			Shutdown(itv, designator.MapTo(0)).
			Shutdown(itv, designator.MapTo(1)).
			Shutdown(itv, designator.MapTo(2)).
			Shutdown(itv, designator.MapTo(3)).
			ShutRestart(itv, restart, designator.MapTo(4), designator.MapTo(0, 1, 2, 3)).
			Restart(itv, restart, designator.MapTo(4)).
			Build()
	case 7:
		return tests.NewSchedulerBuilder(size).
			Init().
			Shutdown(itv, designator.MapTo(0)).
			Shutdown(itv, designator.MapTo(1)).
			Shutdown(itv, designator.MapTo(2)).
			Shutdown(itv, designator.MapTo(3)).
			Shutdown(itv, designator.MapTo(4)).
			Shutdown(itv, designator.MapTo(5)).
			ShutRestart(itv, restart, designator.MapTo(6), designator.MapTo(0, 1, 2, 3, 4, 5)).
			Restart(itv, restart, designator.MapTo(6)).
			Build()
	default:
		return tests.DoNothing
	}
}

func MakeWeakDataLossBScenario(restart func(*tests.CDescriptor, int) (*embed.Etcd, error), size int, itv time.Duration) tests.Scheduler {
	designator := tests.GlobalRunnerConfigs["s_designator"].(*tests.ReshuffledDesignator)

	switch size {
	case 3:
		return tests.NewSchedulerBuilder(size).
			Init().
			Shutdown(itv, designator.MapTo(0)).
			Shutdown(itv, designator.MapTo(1)).
			LoadShutFuncGen("soft", tests.GlobalRunnerConfigs["sch-soft-shut"].(tests.ShutGen)).
			ShutRestart(itv, restart, designator.MapTo(2), designator.MapTo(0, 1)).
			Restart(itv*2, restart, designator.MapTo(2)).
			Build()
	case 5:
		return tests.NewSchedulerBuilder(size).
			Init().
			Shutdown(itv, designator.MapTo(0)).
			Shutdown(itv, designator.MapTo(1)).
			Shutdown(itv, designator.MapTo(2)).
			Shutdown(itv, designator.MapTo(3)).
			LoadShutFuncGen("soft", tests.GlobalRunnerConfigs["sch-soft-shut"].(tests.ShutGen)).
			ShutRestart(itv, restart, designator.MapTo(4), designator.MapTo(0, 1, 2, 3)).
			Restart(itv*2, restart, designator.MapTo(4)).
			Build()
	case 7:
		return tests.NewSchedulerBuilder(size).
			Init().
			Shutdown(itv, designator.MapTo(0)).
			Shutdown(itv, designator.MapTo(1)).
			Shutdown(itv, designator.MapTo(2)).
			Shutdown(itv, designator.MapTo(3)).
			Shutdown(itv, designator.MapTo(4)).
			Shutdown(itv, designator.MapTo(5)).
			LoadShutFuncGen("soft", tests.GlobalRunnerConfigs["sch-soft-shut"].(tests.ShutGen)).
			ShutRestart(itv, restart, designator.MapTo(6), designator.MapTo(0, 1, 2, 3, 4, 5)).
			Restart(itv*2, restart, designator.MapTo(6)).
			Build()
	default:
		return tests.DoNothing
	}
}

func MakeDelayBScenario(restart func(*tests.CDescriptor, int) (*embed.Etcd, error), size int, itv time.Duration) tests.Scheduler {
	designator := tests.GlobalRunnerConfigs["s_designator"].(*tests.ReshuffledDesignator)

	switch size {
	case 3:
		return tests.NewSchedulerBuilder(size).
			Init().
			Shutdown(itv, designator.MapTo(0)).
			Shutdown(itv, designator.MapTo(1)).
			Delay(itv, true, designator.MapTo(2)).
			Restart(itv, restart, designator.MapTo(0, 1)).
			Build()
	case 5:
		return tests.NewSchedulerBuilder(size).
			Init().
			Shutdown(itv, designator.MapTo(0)).
			Shutdown(itv, designator.MapTo(1)).
			Shutdown(itv, designator.MapTo(2)).
			Shutdown(itv, designator.MapTo(3)).
			Delay(itv, true, designator.MapTo(4)).
			Restart(itv, restart, designator.MapTo(0, 1, 2, 3)).
			Build()
	case 7:
		return tests.NewSchedulerBuilder(size).
			Init().
			Shutdown(itv, designator.MapTo(0)).
			Shutdown(itv, designator.MapTo(1)).
			Shutdown(itv, designator.MapTo(2)).
			Shutdown(itv, designator.MapTo(3)).
			Shutdown(itv, designator.MapTo(4)).
			Shutdown(itv, designator.MapTo(5)).
			Delay(itv, true, designator.MapTo(6)).
			Restart(itv, restart, designator.MapTo(0, 1, 2, 3, 4, 5)).
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

	if err := utils.RemoveAllSchLog(); err != nil {
		fmt.Println("cannot remove all sch log: ", err)
		return err
	} else {
		fmt.Println("all past sch log has been removed")
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

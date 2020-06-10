package main

import (
	"fmt"
	"go.etcd.io/etcd/tests/adaptive/tests"
	"go.etcd.io/etcd/tests/adaptive/utils"
	"time"
)

func main() {
	tests.InitRunnerConfig()
	utils.InitClientConfig()

	if err := utils.RemoveAllSrvInfo(); err != nil {
		fmt.Println("cannot remove all srv info: ", err)
		return
	} else {
		fmt.Println("all past srv info has been removed")
	}

	if err := utils.RemoveAllSrvLog(); err != nil {
		fmt.Println("cannot remove all srv log: ", err)
		return
	} else {
		fmt.Println("all past srv log has been removed")
	}

	go utils.CreateBenchShell(5)

	tests.GlobalRunnerConfigs["remain-duration"] = 20 * time.Second

	tester := tests.EtcdServerTestRunner

	sch := tests.NewSchedulerBuilder(5).
		Init().
		Shutdown(20*time.Second, []int{0}).
		Shutdown(20*time.Second, []int{1}).
		Restart(20*time.Second, tester.Restart, []int{0}).
		Restart(20*time.Second, tester.Restart, []int{1}).
		Build()

	tester.Run5(sch)

	// tests.NormalServerTestRunner.Run7(tests.DoNothing)
	// tests.EtcdServerTestRunner.Run5(tests.DoNothing)
}

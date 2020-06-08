package main

import (
	"fmt"
	"go.etcd.io/etcd/tests/adaptive/tests"
	"go.etcd.io/etcd/tests/adaptive/utils"
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

	go utils.CreateBenchShell(5)

	// tests.NormalServerTestRunner.Run7(tests.DoNothing)
	tests.EtcdServerTestRunner.Run5(tests.DoNothing)
}

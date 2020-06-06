package main

import (
	"fmt"
	"go.etcd.io/etcd/tests/adaptive/tests"
	"go.etcd.io/etcd/tests/adaptive/utils"
)

func main() {
	tests.InitRunnerConfig()

	if err := utils.RemoveAllSrvInfo(); err != nil {
		fmt.Println("cannot remove all srv info: ", err)
		return
	} else {
		fmt.Println("all past srv info has been removed")
	}

	// tests.NormalServerTestRunner.Run1(tests.DoNothing)
	tests.EtcdServerTestRunner.Run3(tests.DoNothing)
}

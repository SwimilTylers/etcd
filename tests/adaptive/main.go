package main

import "go.etcd.io/etcd/tests/adaptive/utils"

func main() {
	// utils.NormalServerTestRunner.Run3(utils.DoNothing)
	utils.EtcdServerTestRunner.Run3(utils.DoNothing)
}

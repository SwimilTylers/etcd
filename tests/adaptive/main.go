package main

import "go.etcd.io/etcd/tests/adaptive/utils"

func main() {
	utils.RunLocalCluster3(utils.DonNothing)
}

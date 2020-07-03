package data

import (
	"go.etcd.io/etcd/clientv3"
	"strings"
)

type BenchData interface {
	// Start Running BenchData
	Init(dataSize int, workerNum int, bufferSize int)

	// After all info is collected from servers, BenchData should make conclusions
	InitValidate(dataSize int, workerNum int, bufferSize int)

	// BenchData sends prepared Ops through Request channel
	Requests() <-chan clientv3.Op

	// After receiving replies from servers, clients should mark the sent Ops through Acknowledge channel
	Acknowledge() chan<- struct {
		clientv3.Op
		clientv3.OpResponse
	}

	// After reading kvs from servers, clients should mark the received Ops through Confirm channel
	Confirm() chan<- clientv3.OpResponse

	// Conclusions made by BenchData will be sent through Results channel
	Results() string

	Load(file string) error
	Store(file string) error
}

func GetBenchDataFromString(desc string) BenchData {
	if strings.HasPrefix(desc, "random") {
		opts := ParseRandomDataOptions(strings.TrimPrefix(desc, "random"))
		if opts[RandDataOptMode] == "sequential" {
			return &SequentialRandomData{
				parallelData: GetParallelDataCore(),
				keySize:      opts[RandDataOptKeySize].(int),
				valueSize:    opts[RandDataOptValueSize].(int),
			}
		} else {
			return nil
		}
	} else {
		return &dummyData{}
	}
}

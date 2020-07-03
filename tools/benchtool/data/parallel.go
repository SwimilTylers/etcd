package data

import (
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"strings"
	"sync"
)

type parallelData struct {
	iWg sync.WaitGroup
	cWg sync.WaitGroup

	requests chan clientv3.Op
	raWorker func(wIdx int)
	rcWorker func(wIdx int)

	ack chan struct {
		clientv3.Op
		clientv3.OpResponse
	}
	aWorker func(wIdx int)

	confirm chan clientv3.OpResponse
	cWorker func(wIdx int)

	info     chan string
	infoAll  string
	infoWait sync.WaitGroup
	conclude func() string
}

func (p *parallelData) Init(dataSize int, workerNum int, bufferSize int) {
	p.requests = make(chan clientv3.Op, bufferSize)
	p.ack = make(chan struct {
		clientv3.Op
		clientv3.OpResponse
	}, bufferSize)

	for i := 0; i < workerNum; i++ {
		go func(wIdx int) {
			p.iWg.Add(1)
			defer p.iWg.Done()

			p.raWorker(wIdx)
		}(i)
		go p.aWorker(i)
	}

	go func() {
		p.iWg.Wait()
		close(p.requests)
	}()
	p.initInfoThread(bufferSize)
}

func (p *parallelData) InitValidate(dataSize int, workerNum int, bufferSize int) {
	p.requests = make(chan clientv3.Op, bufferSize)
	p.confirm = make(chan clientv3.OpResponse, bufferSize)

	for i := 0; i < workerNum; i++ {
		go func(wIdx int) {
			p.cWg.Add(1)
			defer p.cWg.Done()

			p.rcWorker(wIdx)
		}(i)
		go p.cWorker(i)
	}

	go func() {
		p.cWg.Wait()
		close(p.requests)
	}()
	p.initInfoThread(bufferSize)
}

func (p *parallelData) initInfoThread(bufferSize int) {
	p.info = make(chan string, bufferSize)
	p.infoWait.Add(1)
	go func() {
		builder := strings.Builder{}
		builder.WriteString("Some info from worker threads:\n")
		defer p.infoWait.Done()
		var entry int
		for s := range p.info {
			builder.WriteString(fmt.Sprintf("[%-3d]: %s\n", entry, s))
			entry++
		}
		builder.WriteString("that's all")
		p.infoAll = builder.String()
	}()
}

func (p *parallelData) Requests() <-chan clientv3.Op {
	return p.requests
}

func (p *parallelData) Acknowledge() chan<- struct {
	clientv3.Op
	clientv3.OpResponse
} {
	return p.ack
}

func (p *parallelData) Confirm() chan<- clientv3.OpResponse {
	return p.confirm
}

func (p *parallelData) Results() string {
	close(p.info)
	builder := strings.Builder{}
	builder.WriteString(p.conclude())
	p.infoWait.Wait()
	builder.WriteString("\n\n")
	builder.WriteString(p.infoAll)
	return builder.String()
}

func (p *parallelData) Load(file string) error {
	return nil
}

func (p *parallelData) Store(file string) error {
	return nil
}

func GetParallelDataCore() *parallelData {
	return &parallelData{}
}

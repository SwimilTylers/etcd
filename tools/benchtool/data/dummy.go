package data

import "go.etcd.io/etcd/clientv3"

type dummyData struct {
	out chan clientv3.Op
	ain chan struct {
		clientv3.Op
		clientv3.OpResponse
	}
	cin chan clientv3.OpResponse
}

func (d *dummyData) Init(dataSize int, workerNum int, bufferSize int) {
	d.out = make(chan clientv3.Op, bufferSize)
	d.ain = make(chan struct {
		clientv3.Op
		clientv3.OpResponse
	}, bufferSize)
	d.cin = make(chan clientv3.OpResponse, bufferSize)

	// dummyData will not generate any requests
	close(d.out)

	for i := 0; i < workerNum; i++ {
		go func() {
			for range d.cin {
			}
		}()
		go func() {
			for range d.ain {
			}
		}()
	}
}

func (d *dummyData) Requests() <-chan clientv3.Op {
	return d.out
}

func (d *dummyData) Acknowledge(clientv3.Op, clientv3.OpResponse) {}

func (d *dummyData) Confirm(clientv3.OpResponse) {}

func (d *dummyData) InitValidate(int, int, int) {}

func (d *dummyData) Results() string {
	return "you dummy"
}

func (d *dummyData) Error(err error) {}

func (d *dummyData) Load(file string) error {
	return nil
}

func (d *dummyData) Store(file string) error {
	return nil
}

func (d *dummyData) Close() error {
	return nil
}

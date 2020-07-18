package data

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

const (
	RandDataOptMode              = "mode"
	RandDataOptKeySize           = "key-size"
	RandDataOptValueSize         = "val-size"
	RandDataOptForceSingleWorker = "force-single"
)

type RDRecords struct {
	values [][]byte
	sent   []bool
}

func (rdr *RDRecords) Marshal() []byte {
	buffer := bytes.NewBuffer(nil)

	cvtBuffer := make([]byte, 8)
	binary.PutVarint(cvtBuffer, int64(len(rdr.values)))
	buffer.Write(cvtBuffer)

	binary.PutVarint(cvtBuffer, int64(len(rdr.values[0])))
	buffer.Write(cvtBuffer)

	for _, v := range rdr.values {
		buffer.Write(v)
	}

	for _, s := range rdr.sent {
		if s {
			binary.PutVarint(cvtBuffer, 1)
			buffer.Write(cvtBuffer)
		} else {
			binary.PutVarint(cvtBuffer, 0)
			buffer.Write(cvtBuffer)
		}
	}

	return buffer.Bytes()
}

func (rdr *RDRecords) Unmarshal(b []byte) error {
	var length int64
	var size int64

	buffer := bytes.NewBuffer(b)

	length, _ = binary.Varint(buffer.Next(8))
	size, _ = binary.Varint(buffer.Next(8))

	rdr.values = make([][]byte, length)
	rdr.sent = make([]bool, length)

	for i := 0; i < len(rdr.values); i++ {
		rdr.values[i] = make([]byte, size)
		if n, err := buffer.Read(rdr.values[i]); err != nil {
			return err
		} else if int64(n) != size {
			return errors.New("not aligned")
		}
	}

	for i := 0; i < len(rdr.sent); i++ {
		signal, _ := binary.Varint(buffer.Next(8))
		rdr.sent[i] = signal == 1
	}

	return nil
}

type SequentialRandomData struct {
	*parallelData

	forceSingleWorker bool

	wNum int

	keySize   int
	valueSize int

	values  [][]byte
	sent    []bool
	checked []bool
}

func (sqd *SequentialRandomData) Init(dataSize int, workerNum int, bufferSize int) {
	sqd.values = make([][]byte, dataSize)
	sqd.sent = make([]bool, dataSize)

	if sqd.forceSingleWorker {
		sqd.wNum = 1
	} else {
		sqd.wNum = workerNum
	}

	sqd.raWorker = func(wIdx int) {
		v := newRandValue(sqd.valueSize)

		gap := sqd.wNum
		for i := wIdx; i < dataSize; i += gap {
			k := make([]byte, sqd.keySize)
			binary.PutVarint(k, int64(i))
			sqd.values[i] = v
			sqd.requests <- clientv3.OpPut(string(k), string(v))
		}
	}

	sqd.aWorker = func(int) {
		for ack := range sqd.ack {
			k, _ := binary.Varint(ack.Op.KeyBytes())
			sqd.sent[k] = bytes.Equal(ack.Op.ValueBytes(), sqd.values[k])
		}
		sqd.done <- struct{}{}
	}

	sqd.parallelData.Init(dataSize, sqd.wNum, bufferSize)
}

func (sqd *SequentialRandomData) InitValidate(dataSize, workerNum, bufferSize int) {
	sqd.checked = make([]bool, dataSize)

	if sqd.forceSingleWorker {
		sqd.wNum = 1
	} else {
		sqd.wNum = workerNum
	}

	sqd.rcWorker = func(wIdx int) {
		gap := sqd.wNum
		for i := wIdx; i < dataSize; i += gap {
			k := make([]byte, sqd.keySize)
			binary.PutVarint(k, int64(i))
			sqd.requests <- clientv3.OpGet(string(k))
		}
	}

	sqd.cWorker = func(int) {
		for resp := range sqd.confirm {
			kvs := resp.Get().Kvs
			for _, kv := range kvs {
				k, _ := binary.Varint(kv.Key)
				sqd.checked[k] = bytes.Equal(kv.Value, sqd.values[k])
			}
		}
		sqd.done <- struct{}{}
	}

	sqd.conclude = func() string {
		var drop int  // sent=false and checked=false
		var weird int // sent=false and checked=true
		var lost int  // sent=true and checked=false
		var succ int  // sent=true and checked=true

		for i := 0; i < len(sqd.values); i++ {
			if sqd.sent[i] {
				if sqd.checked[i] {
					succ++
				} else {
					lost++
				}
			} else {
				if sqd.checked[i] {
					weird++
				} else {
					drop++
				}
			}
		}
		return fmt.Sprintf("Database checking:\ntotal: \t%d\ndrop: \t%d\nweird: \t%d\nlost: \t%d\nsucc: \t%d", len(sqd.values), drop, weird, lost, succ)
	}

	sqd.parallelData.InitValidate(dataSize, sqd.wNum, bufferSize)
}

func (sqd *SequentialRandomData) Requests() <-chan clientv3.Op {
	return sqd.requests
}

func (sqd *SequentialRandomData) Acknowledge(op clientv3.Op, resp clientv3.OpResponse) {
	if sqd.forceSingleWorker {
		k, _ := binary.Varint(op.KeyBytes())
		sqd.sent[k] = bytes.Equal(op.ValueBytes(), sqd.values[k])
	} else {
		sqd.parallelData.Acknowledge(op, resp)
	}
}

func (sqd *SequentialRandomData) Confirm(resp clientv3.OpResponse) {
	if sqd.forceSingleWorker {
		kvs := resp.Get().Kvs
		for _, kv := range kvs {
			k, _ := binary.Varint(kv.Key)
			sqd.checked[k] = bytes.Equal(kv.Value, sqd.values[k])
		}
	} else {
		sqd.Confirm(resp)
	}
}

func (sqd *SequentialRandomData) Load(file string) error {
	f, err := os.Open(file)

	if err != nil {
		return err
	}

	defer f.Close()

	if b, err := ioutil.ReadAll(f); err == nil {
		rdr := &RDRecords{}
		err = rdr.Unmarshal(b)

		if err != nil {
			return err
		}

		sqd.values = rdr.values
		sqd.sent = rdr.sent

		return nil
	} else {
		return err
	}
}

func (sqd *SequentialRandomData) Store(file string) error {
	store := &RDRecords{
		values: sqd.values,
		sent:   sqd.sent,
	}

	b := store.Marshal()
	f, err := os.Create(file)

	if err != nil {
		return err
	}

	defer f.Close()

	_, err = f.Write(b)
	return err
}

func newRandValue(vSize int) []byte {
	rb := make([]byte, vSize)
	_, err := rand.Read(rb)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate value: %v\n", err)
		os.Exit(1)
	}
	return rb
}

func ParseRandomDataOptions(opts string) map[string]interface{} {
	opts = strings.Trim(opts, "[ ]")
	ret := GetDefaultRandomDataOptions()
	for _, opt := range strings.Split(opts, ",") {
		kvs := strings.Split(opt, "=")
		switch kvs[0] {
		case RandDataOptMode:
			ret[RandDataOptMode] = kvs[1]
		case RandDataOptKeySize:
			ret[RandDataOptKeySize], _ = strconv.Atoi(kvs[1])
		case RandDataOptValueSize:
			ret[RandDataOptValueSize], _ = strconv.Atoi(kvs[1])
		case RandDataOptForceSingleWorker:
			ret[RandDataOptForceSingleWorker], _ = strconv.ParseBool(kvs[1])
		}
	}

	return ret
}

func GetDefaultRandomDataOptions() map[string]interface{} {
	ret := make(map[string]interface{})

	ret[RandDataOptMode] = "sequential"
	ret[RandDataOptKeySize] = 8
	ret[RandDataOptValueSize] = 8
	ret[RandDataOptForceSingleWorker] = false

	return ret
}

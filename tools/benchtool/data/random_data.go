package data

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"os"
	"strconv"
	"strings"
)

const (
	RandDataOptMode      = "mode"
	RandDataOptKeySize   = "key-size"
	RandDataOptValueSize = "val-size"
)

type SequentialRandomData struct {
	*parallelData

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
	sqd.checked = make([]bool, dataSize)

	sqd.wNum = workerNum

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

	sqd.rcWorker = func(wIdx int) {
		gap := sqd.wNum
		for i := wIdx; i < dataSize; i += gap {
			k := make([]byte, sqd.keySize)
			binary.PutVarint(k, int64(i))
			sqd.requests <- clientv3.OpGet(string(k))
		}
	}

	sqd.aWorker = func(int) {
		for ack := range sqd.ack {
			k, _ := binary.Varint(ack.Op.KeyBytes())
			sqd.sent[k] = bytes.Equal(ack.Op.ValueBytes(), sqd.values[k])
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

	sqd.parallelData.Init(dataSize, workerNum, bufferSize)
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
		}
	}

	return ret
}

func GetDefaultRandomDataOptions() map[string]interface{} {
	ret := make(map[string]interface{})

	ret[RandDataOptMode] = "sequential"
	ret[RandDataOptKeySize] = 8
	ret[RandDataOptValueSize] = 8

	return ret
}

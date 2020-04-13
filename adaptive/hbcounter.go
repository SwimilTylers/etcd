package adaptive

import (
	"bytes"
	"fmt"
)

type HeartbeatCounter interface {
	Positive()
	Negative()
	Report() bool
	Init(positive bool)
}

type SimpleBucketCounter struct {
	counter  int
	maxToken int
}

func (sbc *SimpleBucketCounter) Init(positive bool) {
	sbc.counter = 0
}

func (sbc *SimpleBucketCounter) String() string {
	var buffer bytes.Buffer
	for i := 0; i < sbc.maxToken; i++ {
		if i < sbc.counter {
			buffer.WriteByte('+')
		} else {
			buffer.WriteByte('-')
		}
	}
	return buffer.String()
}

func (sbc *SimpleBucketCounter) Positive() {
	if sbc.counter >= sbc.maxToken {
		sbc.counter = sbc.maxToken
	} else {
		sbc.counter++
	}
}

func (sbc *SimpleBucketCounter) Negative() {
	if sbc.counter <= 0 {
		sbc.counter = 0
	} else {
		sbc.counter--
	}
}

func (sbc *SimpleBucketCounter) Report() bool {
	return sbc.counter > 0
}

func NewSimpleBucketCounterFactory(maxToken int) func() HeartbeatCounter {
	return func() HeartbeatCounter {
		return &SimpleBucketCounter{maxToken: maxToken}
	}
}

type CustomizedBucketCounterConfig struct {
	N       int
	wPlus   int
	wMinus  int
	xiPlus  int
	xiMinus int
	t       int
}

var NaiveCounterCfg = &CustomizedBucketCounterConfig{
	N:       2,
	wPlus:   1,
	wMinus:  1,
	xiPlus:  2,
	xiMinus: 0,
	t:       0,
}

var BoldCounterCfg = &CustomizedBucketCounterConfig{
	N:       100,
	wPlus:   30,
	wMinus:  10,
	xiPlus:  100,
	xiMinus: 0,
	t:       55,
}

var CautiousCounterCfg = &CustomizedBucketCounterConfig{
	N:       100,
	wPlus:   30,
	wMinus:  10,
	xiPlus:  100,
	xiMinus: 0,
	t:       75,
}

type CustomizedBucketCounter struct {
	counter int
	config  *CustomizedBucketCounterConfig
}

func (cbc *CustomizedBucketCounter) String() string {
	return fmt.Sprintf("cbc=%d/%v,[%d,%d,%d,%d,%d,%d]",
		cbc.counter, cbc.Report(), cbc.config.N, cbc.config.wPlus, cbc.config.wMinus,
		cbc.config.xiPlus, cbc.config.xiMinus, cbc.config.t,
	)
}

func (cbc *CustomizedBucketCounter) Positive() {
	if cbc.counter < cbc.config.N {
		cbc.counter += cbc.config.wPlus
		if cbc.counter > cbc.config.N {
			cbc.counter = cbc.config.N
		}
	}
}

func (cbc *CustomizedBucketCounter) Negative() {
	if cbc.counter > 0 {
		cbc.counter -= cbc.config.wMinus
		if cbc.counter < 0 {
			cbc.counter = 0
		}
	}
}

func (cbc *CustomizedBucketCounter) Report() bool {
	return cbc.counter > cbc.config.t
}

func (cbc *CustomizedBucketCounter) Init(positive bool) {
	if positive {
		cbc.counter = cbc.config.xiPlus
	} else {
		cbc.counter = cbc.config.xiMinus
	}
}

func NaiveHbCounterFactory() HeartbeatCounter {
	return &CustomizedBucketCounter{
		counter: 0,
		config:  NaiveCounterCfg,
	}
}

func BoldHbCounterFactory() HeartbeatCounter {
	return &CustomizedBucketCounter{
		counter: 0,
		config:  BoldCounterCfg,
	}
}

func CautiousHbCounterFactory() HeartbeatCounter {
	return &CustomizedBucketCounter{
		counter: 0,
		config:  CautiousCounterCfg,
	}
}

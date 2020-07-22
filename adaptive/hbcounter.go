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
	counter    int
	lastResult bool

	// if polarize=true, when lastResult != Report(), reset counter to xiPlus/xiMinus
	polarize bool

	config *CustomizedBucketCounterConfig
}

func (cbc *CustomizedBucketCounter) String() string {
	return fmt.Sprintf("cbc=%d,[%d,%d,%d,%d,%d,%d],last=%v,polarized=%v",
		cbc.counter, cbc.config.N, cbc.config.wPlus, cbc.config.wMinus,
		cbc.config.xiPlus, cbc.config.xiMinus, cbc.config.t, cbc.lastResult, cbc.polarize,
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
	result := cbc.counter > cbc.config.t
	if cbc.polarize && cbc.lastResult != result {
		cbc.Init(result)
	} else {
		cbc.lastResult = result
	}
	return cbc.lastResult
}

func (cbc *CustomizedBucketCounter) Init(positive bool) {
	if positive {
		cbc.counter = cbc.config.xiPlus
		cbc.lastResult = positive
	} else {
		cbc.counter = cbc.config.xiMinus
		cbc.lastResult = positive
	}
}

type DummyHbCounter struct {
	always bool
}

func (d *DummyHbCounter) Positive() {}

func (d *DummyHbCounter) Negative() {}

func (d *DummyHbCounter) Report() bool {
	return d.always
}

func (d *DummyHbCounter) Init(positive bool) {}

type BipolarHbCounter struct {
	critical bool
	counter  int

	bipolar [2]int
}

func (bhc *BipolarHbCounter) Positive() {
	if !bhc.critical {
		// if not critical, bhc will reset to bipolar[1]
		bhc.counter = bhc.bipolar[1]
	} else {
		// otherwise, bhc will count down
		bhc.counter--
	}
}

func (bhc *BipolarHbCounter) Negative() {
	if bhc.critical && bhc.counter < bhc.bipolar[0] {
		// if is critical, bhc will increment counter with upper bound of bipolar[0]
		bhc.counter++
	} else {
		// otherwise, bhc will count down
		bhc.counter--
	}
}

func (bhc *BipolarHbCounter) Report() bool {
	if bhc.counter <= 0 {
		bhc.Init(bhc.critical)
	}
	return !bhc.critical
}

func (bhc *BipolarHbCounter) Init(positive bool) {
	if positive {
		bhc.critical = false
		bhc.counter = bhc.bipolar[1]
	} else {
		bhc.critical = true
		bhc.counter = bhc.bipolar[0]
	}
}

func NaiveHbCounterFactory() HeartbeatCounter {
	return &CustomizedBucketCounter{
		counter:  0,
		config:   NaiveCounterCfg,
		polarize: false,
	}
}

func BoldHbCounterFactory() HeartbeatCounter {
	return &CustomizedBucketCounter{
		counter:  0,
		config:   BoldCounterCfg,
		polarize: false,
	}
}

func CautiousHbCounterFactory() HeartbeatCounter {
	return &CustomizedBucketCounter{
		counter:  0,
		config:   CautiousCounterCfg,
		polarize: false,
	}
}

func PolarizedNaiveHbCounterFactory() HeartbeatCounter {
	return &CustomizedBucketCounter{
		counter:  0,
		config:   NaiveCounterCfg,
		polarize: true,
	}
}

func PolarizedBoldHbCounterFactory() HeartbeatCounter {
	return &CustomizedBucketCounter{
		counter:  0,
		config:   BoldCounterCfg,
		polarize: true,
	}
}

func PolarizedCautiousHbCounterFactory() HeartbeatCounter {
	return &CustomizedBucketCounter{
		counter:  0,
		config:   CautiousCounterCfg,
		polarize: true,
	}
}

func AlwaysConnectHbCounterFactory() HeartbeatCounter {
	return &DummyHbCounter{true}
}

func AlwaysDisconnectHbCounterFactory() HeartbeatCounter {
	return &DummyHbCounter{false}
}

func BipolarHbCounterFactoryGenerator(bPlus int, bMinus int) func() HeartbeatCounter {
	return func() HeartbeatCounter {
		return &BipolarHbCounter{
			critical: true,
			counter:  bMinus,
			bipolar:  [2]int{bMinus, bPlus},
		}
	}
}

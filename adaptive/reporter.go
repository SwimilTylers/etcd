package adaptive

import "bytes"

type Reporter interface {
	Positive()
	Negative()
	Report() bool
}

type TokenBucketReporter struct {
	counter  int
	maxToken int
}

func (tbr *TokenBucketReporter) String() string {
	var buffer bytes.Buffer
	for i := 0; i < tbr.maxToken; i++ {
		if i < tbr.counter {
			buffer.WriteByte('+')
		} else {
			buffer.WriteByte('-')
		}
	}
	return buffer.String()
}

func (tbr *TokenBucketReporter) Positive() {
	if tbr.counter >= tbr.maxToken {
		tbr.counter = tbr.maxToken
	} else {
		tbr.counter++
	}
}

func (tbr *TokenBucketReporter) Negative() {
	if tbr.counter <= 0 {
		tbr.counter = 0
	} else {
		tbr.counter--
	}
}

func (tbr *TokenBucketReporter) Report() bool {
	return tbr.counter > 0
}

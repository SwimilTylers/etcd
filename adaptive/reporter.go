package adaptive

type TokenBucketReporter struct {
	counter  int
	maxToken int
}

func (cr *TokenBucketReporter) Positive() {
	if cr.counter >= cr.maxToken {
		cr.counter = cr.maxToken
	} else {
		cr.counter++
	}
}

func (cr *TokenBucketReporter) Negative() {
	if cr.counter <= 0 {
		cr.counter = 0
	} else {
		cr.counter--
	}
}

func (cr *TokenBucketReporter) Test() bool {
	return cr.counter > 0
}

package adaptive

type ConnectionReporter struct {
	counter  int
	maxToken int
}

func (cr *ConnectionReporter) Positive() {
	if cr.counter >= cr.maxToken {
		cr.counter = cr.maxToken
	} else {
		cr.counter++
	}
}

func (cr *ConnectionReporter) Negative() {
	if cr.counter <= 0 {
		cr.counter = 0
	} else {
		cr.counter--
	}
}

func (cr *ConnectionReporter) Gather() bool {
	return cr.counter > 0
}

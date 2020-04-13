package adaptive

import (
	"testing"
)

func TestCautiousHbCounter(t *testing.T) {
	hbc := CautiousHbCounterFactory()

	// test b_plus ?= 3

	hbc.Init(true)
	for i := 1; i <= 5; i++ {
		hbc.Negative()
		if i < 3 && !hbc.Report() {
			t.Error("too early to trigger")
		} else if i >= 3 && hbc.Report() {
			t.Error("too late to trigger")
		}
	}

	// test b_minus ?= 4

	hbc.Init(false)
	for i := 1; i <= 10; i++ {
		hbc.Negative()
		if i >= 5 && !hbc.Report() {
			t.Error("unstable trigger")
		}
		hbc.Positive()
		if i < 4 && hbc.Report() {
			t.Error("too early to trigger")
		} else if i >= 4 && !hbc.Report() {
			t.Error("too late to trigger")
		}
	}
}

package adaptive

import (
	"fmt"
	"math/rand"
	"strings"
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

func TestBipolarHbCounter(t *testing.T) {
	for i := 0; i < 100; i++ {
		bPlus, bMinus := rand.Int()%10+1, rand.Int()%10+1
		counter := BipolarHbCounterFactoryGenerator(bPlus, bMinus)()

		if counter.Report() {
			t.Error("should init as false", printParams(map[string]interface{}{"bPlus": bPlus, "bMinus": bMinus}))
		}

		if counter.Init(true); !counter.Report() {
			t.Error("should reset to true", printParams(map[string]interface{}{"bPlus": bPlus, "bMinus": bMinus}))
		}

		if counter.Init(false); counter.Report() {
			t.Error("should reset to false", printParams(map[string]interface{}{"bPlus": bPlus, "bMinus": bMinus}))
		}

		// test when critical=false

		for i := 1; i < bMinus; i++ {
			counter.Negative()
			counter.Positive()

			if counter.Report() {
				t.Error("too acute to sense mode switch, i =", i, printParams(map[string]interface{}{"bPlus": bPlus, "bMinus": bMinus}))
			}
		}

		counter.Negative()
		counter.Positive()

		if !counter.Report() {
			t.Error("too dumb to sense mode switch", printParams(map[string]interface{}{"bPlus": bPlus, "bMinus": bMinus}))
		}

		// test when critical=true

		for i := 1; i < bPlus; i++ {
			counter.Negative()

			if !counter.Report() {
				t.Error("too acute to sense mode switch, i =", i, printParams(map[string]interface{}{"bPlus": bPlus, "bMinus": bMinus}))
			}
		}

		counter.Negative()

		if counter.Report() {
			t.Error("too dumb to sense mode switch", printParams(map[string]interface{}{"bPlus": bPlus, "bMinus": bMinus}))
		}

		// bipolar

		if bMinus > 1 {
			for i := 1; i < bMinus; i++ {
				counter.Negative()
				counter.Positive()
			}

			counter.Negative()
			counter.Positive()

			counter.Negative()

			if !counter.Report() {
				t.Error("too acute to sense mode switch", printParams(map[string]interface{}{"bPlus": bPlus, "bMinus": bMinus}))
			}
		}

	}
}

func printParams(param map[string]interface{}) string {
	s := strings.Builder{}
	s.WriteString("[")
	for k, v := range param {
		s.WriteString(fmt.Sprintf("%s=%v,", k, v))
	}
	s.WriteString("]")
	return s.String()
}

package utils

type TestRunner struct {
	Run1 func(s Scheduler)
	Run3 func(s Scheduler)
	Run5 func(s Scheduler)
	Run7 func(s Scheduler)
}

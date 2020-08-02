package tests

import "math/rand"

type CrashDesignatorAction uint8

const (
	CDActionNop CrashDesignatorAction = iota
	CDActionCrash
	CDActionRestart
	CDActionStop
)

type CrashDesignator interface {
	NextCrash(s int) []int
	NextRestart(s int) []int

	NextAction() (CrashDesignatorAction, []int)
}

type SimpleDesignator struct {
	size int
	cIdx int
	rIdx int

	perm []int
}

func (sd *SimpleDesignator) NextCrash(s int) []int {
	var ret []int
	for count := 0; count < s; count++ {
		ret = append(ret, sd.perm[sd.cIdx])
		sd.cIdx++
		if sd.cIdx >= sd.size {
			break
		}
	}
	return ret
}

func (sd *SimpleDesignator) NextRestart(s int) []int {
	var ret []int
	for count := 0; count < s; count++ {
		ret = append(ret, sd.perm[sd.rIdx])
		sd.rIdx++
		if sd.rIdx >= sd.size {
			break
		}
	}
	return ret
}

func (sd *SimpleDesignator) NextAction() (CrashDesignatorAction, []int) {
	return CDActionStop, nil
}

func NewDesignator(size int, pGenerator func(s int) []int) *SimpleDesignator {
	return &SimpleDesignator{
		size: size,
		cIdx: 0,
		rIdx: 0,
		perm: pGenerator(size),
	}
}

var (
	SequentialDesignatorPermGenerator = func(s int) []int {
		perm := make([]int, s)
		for i := 0; i < s; i++ {
			perm[i] = i
		}
		return perm
	}
	RandomDesignatorPermGenerator = func(s int) []int {
		return rand.Perm(s)
	}
)

type RandomWalkDesignator struct {
	size  int
	async bool

	survival []bool
}

func (rwd *RandomWalkDesignator) NextCrash(s int) []int {
	panic("implement me")
}

func (rwd *RandomWalkDesignator) NextRestart(s int) []int {
	panic("implement me")
}

func (rwd *RandomWalkDesignator) NextAction() (CrashDesignatorAction, []int) {
	panic("implement me")
}

func NewRandomWalkDesignator(size int, async bool) *RandomWalkDesignator {
	return nil
}

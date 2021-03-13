package draft

//CEC2EFCxMerge implements a merging between ConsecutiveEntryCollector and EntryFragmentCollector.
// The major collector must be a ConsecutiveEntryCollector. The first minor collector should be a
// ConsecutiveEntryCollector as well. The rest should be EntryFragmentCollector, omitted if not.
func CEC2EFCxMerge(commit uint64, major Collector, minor []Collector, minorCommit []uint64) uint64 {
	// merge the first minor collector
	if !minor[0].IsEmpty() {
		if ok, ent, lt, li := minor[0].FetchAllEntries(); ok {
			major.AddEntries(ent, lt, li)
		}
		commit = minorCommit[0]
	}

	// merge the other minor collectors
	var cc = major.(*ConsecutiveEntryCollector)
	var fc []*EntryFragmentCollector
	for i, c := range minor {
		if i == 0 {
			// skip the first minor collector
			continue
		}

		if c.IsEmpty() {
			// skip empty collector
			continue
		}

		if fcc, ok := c.(*EntryFragmentCollector); ok {
			fc = append(fc, fcc)
		}
	}

	// merge fragments
	MergeEntryFragments(commit, fc, cc)

	// merge commit index
	for _, c := range minorCommit {
		if c > commit {
			commit = c
		}
	}

	return commit
}

//MergeEntryFragments merges in-collectors to out-collector.
func MergeEntryFragments(commit uint64, in []*EntryFragmentCollector, out *ConsecutiveEntryCollector) {
	inLen := len(in)

	if inLen == 0 {
		return
	}

	fragments := make([][]*EntryFragment, 0, inLen)
	fIndices := make([]int, 0, inLen)

	_, initTerm := out.GetLatestTerm()

	// filter out out-of-date fragments
	for _, c := range in {
		if ok, fragment := c.FetchFragmentsWithStartIndex(commit + 1); ok {
			idx := -1
			for j, f := range fragment {
				if f.latestTerm >= initTerm {
					idx = j
				}
			}
			if idx != -1 {
				fragments = append(fragments, fragment)
				fIndices = append(fIndices, idx)
			}
		}
	}

	count := len(fragments)

	// merging
	for count > 0 {
		min := 0
		minTerm := fragments[0][fIndices[0]].latestTerm

		for i := 1; i < inLen; i++ {
			index := fIndices[i]
			if index == len(fragments[i]) {
				continue
			}
			term := fragments[i][index].latestTerm
			if term < minTerm {
				min = i
				minTerm = term
			}
		}

		f := fragments[min][fIndices[min]]
		fIndices[min]++
		if fIndices[min] == len(fragments[min]) {
			count--
		}

		out.AddEntries(f.fragment, f.logTerm, f.logIndex)
	}
}

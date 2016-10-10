package bard

func jch(key uint64, buckets int64) int64 {
	var j int64 = 0
	var b int64 = -1
	var m uint64 = 1

	for j < buckets {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(uint64(b+1) * (m << 31 / ((key >> 33) + 1)))
	}

	return b
}

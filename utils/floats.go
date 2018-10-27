package utils

import (
	"math"
	"sort"
)

// Sort sorts a slice of floats.
func Sort(v []float64) {
	if !sort.Float64sAreSorted(v) {
		sort.Float64s(v)
	}
}

// Min gets the minimum from a slice of floats.
func Min(v []float64) float64 {
	Sort(v)

	return v[0]
}

// Percentile gets the given percentile of from a slice of floats.
func Percentile(v []float64, perc float64) float64 {
	Sort(v)

	pos := int(math.Floor(float64(len(v)) * (perc / 100)))
	return v[pos]
}

// Max gets the maximum value from a slice of floats.
func Max(v []float64) float64 {
	Sort(v)

	return v[len(v)-1]
}

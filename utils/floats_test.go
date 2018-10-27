package utils_test

import (
	"testing"

	"github.com/nrwiersma/snatch/utils"
	"github.com/stretchr/testify/assert"
)

func TestSort(t *testing.T) {
	v := []float64{4, 1, 5, 3, 2}

	utils.Sort(v)

	assert.Equal(t, []float64{1, 2, 3, 4, 5}, v)
}

func TestMin(t *testing.T) {
	v := []float64{4, 1, 5, 3, 2}

	m := utils.Min(v)

	assert.Equal(t, float64(1), m)
}

func TestMax(t *testing.T) {
	v := []float64{4, 1, 5, 3, 2}

	m := utils.Max(v)

	assert.Equal(t, float64(5), m)
}

func TestPercentile(t *testing.T) {
	v := make([]float64, 100)
	for i := 1; i < 100; i++ {
		v[i-1] = float64(i)
	}

	m := utils.Percentile(v, 98)

	assert.Equal(t, float64(98), m)
}

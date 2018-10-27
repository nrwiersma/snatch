package snatch_test

import (
	"testing"
	"time"

	"github.com/nrwiersma/snatch"
	"github.com/stretchr/testify/assert"
)

func TestId_Keys(t *testing.T) {
	id := &snatch.ID{
		Time: time.Unix(414631410, 0),
		Name: "test",
		Tags: []string{"foo", "bar"},
		Type: "counter",
	}

	ts, key := id.Keys()

	assert.Equal(t, int64(414631410), ts)
	assert.Equal(t, "counter:test:foo,bar", key)
}

func BenchmarkId_Keys(b *testing.B) {
	id := &snatch.ID{
		Time: time.Unix(414631410, 0),
		Name: "test",
		Tags: []string{"foo", "bar"},
		Type: "counter",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id.Keys()
	}
}

func TestBucket_Append(t *testing.T) {
	b := &snatch.Bucket{}

	b.Append(1.2)
	b.Append(0.8)

	assert.Equal(t, []float64{1.2, 0.8}, b.Vals)
	assert.Equal(t, 2.0, b.Sum)
}

func TestBucket_Merge(t *testing.T) {
	b := &snatch.Bucket{}
	b.Append(1.2)

	b2 := &snatch.Bucket{}
	b2.Append(0.8)

	b.Merge(b2)

	assert.Equal(t, []float64{1.2, 0.8}, b.Vals)
	assert.Equal(t, 2.0, b.Sum)
}

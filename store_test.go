package snatch_test

import (
	"testing"
	"time"

	"github.com/nrwiersma/snatch"
	"github.com/stretchr/testify/assert"
)

func TestNewStore(t *testing.T) {
	s := snatch.NewStore(10 * time.Second)

	assert.Implements(t, (*snatch.Store)(nil), s)
}

func TestMemStore_Put(t *testing.T) {
	bkts := []*snatch.Bucket{
		{
			ID: &snatch.ID{
				Time: time.Now().Truncate(time.Second).Add(-1 * time.Second),
				Name: "foo",
				Type: "count",
			},
			Vals: []float64{1},
			Sum:  1,
		},
		{
			ID: &snatch.ID{
				Time: time.Now().Truncate(time.Second).Add(-1 * time.Second),
				Name: "bar",
				Type: "measure",
			},
			Vals: []float64{2},
			Sum:  2,
		},
		{
			ID: &snatch.ID{
				Time: time.Now().Truncate(time.Second).Add(-1 * time.Second),
				Name: "bar",
				Type: "measure",
			},
			Vals: []float64{3},
			Sum:  3,
		},
		{
			ID: &snatch.ID{
				Time: time.Now().Truncate(time.Second).Add(-1 * time.Second),
				Name: "bar",
				Type: "measure",
			},
			Vals: []float64{3},
			Sum:  3,
		},
	}
	s := snatch.NewStore(10 * time.Second)

	expired, err := s.Add(bkts...)

	assert.NoError(t, err)
	assert.Equal(t, 0, expired)
	out, _ := s.Flush()
	var sum float64
	for bkt := range out {
		sum += bkt.Sum
	}
	assert.Equal(t, float64(9), sum)

}

func TestMemStore_PutExpired(t *testing.T) {
	bkts := []*snatch.Bucket{
		{
			ID: &snatch.ID{
				Time: time.Now().Truncate(time.Second).Add(-1 * time.Minute),
				Name: "foo",
				Type: "count",
			},
			Vals: []float64{1},
			Sum:  1,
		},
		{
			ID: &snatch.ID{
				Time: time.Now().Truncate(time.Second).Add(-1 * time.Minute),
				Name: "bar",
				Type: "measure",
			},
			Vals: []float64{2},
			Sum:  2,
		},
		{
			ID: &snatch.ID{
				Time: time.Now().Truncate(time.Second).Add(-1 * time.Minute),
				Name: "bar",
				Type: "measure",
			},
			Vals: []float64{3},
			Sum:  3,
		},
		{
			ID: &snatch.ID{
				Time: time.Now().Truncate(time.Second).Add(-1 * time.Second),
				Name: "bar",
				Type: "measure",
			},
			Vals: []float64{3},
			Sum:  3,
		},
	}
	s := snatch.NewStore(10 * time.Second)

	expired, err := s.Add(bkts...)

	assert.NoError(t, err)
	assert.Equal(t, 3, expired)

}

func BenchmarkMemStore_Put(b *testing.B) {
	s := snatch.NewStore(10 * time.Second)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Add(&snatch.Bucket{
			ID: &snatch.ID{
				Time: time.Now(),
				Name: "foo",
				Type: "count",
			},
			Vals: []float64{1},
			Sum:  1,
		})
	}
}

func TestMemStore_Scan(t *testing.T) {
	s := snatch.NewStore(10 * time.Second)

	bkt := &snatch.Bucket{
		ID: &snatch.ID{
			Time: time.Now().Add(-10 * time.Second),
			Name: "foo",
			Type: "test",
		},
	}
	bkt.Append(2.353)
	expired, err := s.Add(bkt)
	assert.NoError(t, err)
	assert.Equal(t, 0 ,expired)
	bkt = &snatch.Bucket{
		ID: &snatch.ID{
			Time: time.Now(),
			Name: "foo",
			Type: "test",
		},
	}
	bkt.Append(2.353)
	expired, err = s.Add(bkt)
	assert.NoError(t, err)
	assert.Equal(t, 0 ,expired)

	time.Sleep(2 * time.Second)

	out, err := s.Scan()

	var b []*snatch.Bucket
	for bkt := range out {
		b = append(b, bkt)
	}
	assert.NoError(t, err)
	assert.Len(t, b, 1)
	assert.Equal(t, float64(2.353), b[0].Sum)
}

func TestMemStore_Flush(t *testing.T) {
	s := snatch.NewStore(10 * time.Second)

	bkt := &snatch.Bucket{
		ID: &snatch.ID{
			Time: time.Now(),
			Name: "foo",
			Type: "test",
		},
	}
	bkt.Append(2.353)
	_, err := s.Add(bkt)
	assert.NoError(t, err)

	out, err := s.Flush()

	var b []*snatch.Bucket
	for bkt := range out {
		b = append(b, bkt)
	}
	assert.NoError(t, err)
	assert.Len(t, b, 1)
	assert.Equal(t, float64(2.353), b[0].Sum)
}

func TestMemStore_CanConcurrentlyPutAndScan(t *testing.T) {
	s := snatch.NewStore(10 * time.Second)
	_, _ = s.Add(&snatch.Bucket{
		ID: &snatch.ID{
			Time: time.Now().Add(-10 * time.Second),
			Name: "foo",
			Type: "count",
		},
		Vals: []float64{1},
		Sum:  1,
	})

	time.Sleep(2 * time.Second)

	done := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-done:
				return

			default:
				_, _ = s.Add(&snatch.Bucket{
					ID: &snatch.ID{
						Time: time.Now(),
						Name: "foo",
						Type: "count",
					},
					Vals: []float64{1},
					Sum:  1,
				})
			}
		}
	}()

	out, _ := s.Scan()
	var b []*snatch.Bucket
	for bkt := range out {
		b = append(b, bkt)
	}
	assert.Len(t, b, 1)

	done <- struct{}{}
}

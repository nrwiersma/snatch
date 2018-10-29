package snatch_test

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/nrwiersma/snatch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewDB(t *testing.T) {
	c := new(mockClient)

	db := snatch.NewDB(c, "testdb")

	assert.Implements(t, (*snatch.DB)(nil), db)
}

func TestInfluxDB_Insert(t *testing.T) {
	bkts := []*snatch.Bucket{
		{
			ID: &snatch.ID{
				Time: time.Now().Truncate(time.Minute),
				Name: "foo.bar.counter",
				Tags: []string{"tag", "example"},
				Type: snatch.Count,
			},
			Vals: []float64{1, 2, 3, 4},
			Sum:  10,
		},
		{
			ID: &snatch.ID{
				Time: time.Now().Truncate(time.Minute),
				Name: "foo.bar.sample",
				Tags: []string{"tag", "example"},
				Type: snatch.Sample,
			},
			Vals: []float64{1, 2, 3, 4},
			Sum:  10,
		},
		{
			ID: &snatch.ID{
				Time: time.Now().Truncate(time.Minute),
				Name: "foo.bar.measure",
				Tags: []string{"tag", "example"},
				Type: snatch.Measure,
			},
			Vals: []float64{1, 2, 3, 4},
			Sum:  10,
		},
	}

	c := new(mockClient)
	c.On("Write", mock.Anything).Run(func(args mock.Arguments) {
		bp := args.Get(0).(client.BatchPoints)
		ps := bp.Points()

		p, _ := client.NewPoint(
			"foo_bar_counter",
			map[string]string{"tag": "example"},
			map[string]interface{}{"value": float64(10)},
			time.Now().Truncate(time.Minute),
		)
		assert.Equal(t, p, ps[0])

		p, _ = client.NewPoint(
			"foo_bar_sample",
			map[string]string{"tag": "example"},
			map[string]interface{}{"value": float64(4)},
			time.Now().Truncate(time.Minute),
		)
		assert.Equal(t, p, ps[1])

		p, _ = client.NewPoint(
			"foo_bar_measure",
			map[string]string{"tag": "example"},
			map[string]interface{}{
				"90_percentile": float64(4),
				"95_percentile": float64(4),
				"97_percentile": float64(4),
				"99_percentile": float64(4),
				"count":         4,
				"lower":         float64(1),
				"mean":          float64(2.5),
				"sum":           float64(10),
				"upper":         float64(4),
			},
			time.Now().Truncate(time.Minute),
		)
		assert.Equal(t, p, ps[2])

	}).Return(nil)
	db := snatch.NewDB(c, "testdb")

	err := db.Insert(bkts)

	assert.NoError(t, err)
}

func TestInfluxDB_Close(t *testing.T) {
	c := new(mockClient)
	c.On("Close").Return(nil)
	db := snatch.NewDB(c, "testdb")

	err := db.Close()

	assert.NoError(t, err)
}

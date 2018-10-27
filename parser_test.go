package snatch_test

import (
	"testing"
	"time"

	"github.com/nrwiersma/snatch"
	"github.com/stretchr/testify/assert"
)

func TestNewParser(t *testing.T) {
	p := snatch.NewParser(time.Second)

	assert.IsType(t, &snatch.Parser{}, p)
}

func TestParser_ParseCanParseValidMetrics(t *testing.T) {
	metrics := [][]byte{
		[]byte("t=\"1983-02-21T01:23:45-0400\" lvl=info msg= count#test=2 foo=\"bar\" size=10"),
		[]byte("t=\"1983-02-21T01:23:45-0400\" lvl=info msg= count#test@0.1=2 foo=\"bar\" size=10"),
		[]byte("t=\"1983-02-21T01:23:45-0400\" lvl=info msg= count#test=-2 foo=\"bar\" size=10"),
		[]byte("t=\"1983-02-21T01:23:45-0400\" lvl=info msg= count#test@0.1=-2 foo=\"bar\" size=10"),
		[]byte("t=\"1983-02-21T01:23:45-0400\" lvl=info msg= sample#test=2.3 foo=\"bar\" size=10"),
		[]byte("t=\"1983-02-21T01:23:45-0400\" lvl=info msg= sample#test@0.1=2.3 foo=\"bar\" size=10"),
		[]byte("t=\"1983-02-21T01:23:45-0400\" lvl=info msg= measure#test=2.3ms foo=\"bar\" size=10"),
		[]byte("t=\"1983-02-21T01:23:45-0400\" lvl=info msg= measure#test@0.1=2.3ms foo=\"bar\" size=10"),
	}

	for _, m := range metrics {
		p := snatch.NewParser(time.Second)

		bkts, err := p.Parse(m)

		assert.NoError(t, err)
		assert.Len(t, bkts, 1)
	}
}

func TestParser_ParseErrorsOnInvalidMetrics(t *testing.T) {
	metrics := [][]byte{
		[]byte("count#test=test"),
		[]byte("foo#test=1.2"),
		[]byte("count#=1.2"),
		[]byte("sample#test=1-2"),
		[]byte("count#test=\"1.2"),
	}

	for _, m := range metrics {
		p := snatch.NewParser(time.Second)

		_, err := p.Parse(m)

		assert.Error(t, err)
	}
}

func TestParser_ParseHandlesCount(t *testing.T) {
	m := []byte("count#prefix.test=2")
	p := snatch.NewParser(30 * time.Second)

	bkts, err := p.Parse(m)

	assert.NoError(t, err)
	assert.Len(t, bkts, 1)
	assert.Equal(t, "prefix.test", bkts[0].ID.Name)
	assert.Equal(t, []float64{2}, bkts[0].Vals)
}

func TestParser_ParseHandlesSample(t *testing.T) {
	m := []byte("sample#prefix.test=2.5")
	p := snatch.NewParser(30 * time.Second)

	bkts, err := p.Parse(m)

	assert.NoError(t, err)
	assert.Len(t, bkts, 1)
	assert.Equal(t, "prefix.test", bkts[0].ID.Name)
	assert.Equal(t, []float64{2.5}, bkts[0].Vals)
}

func TestParser_ParseHandlesMeasure(t *testing.T) {
	m := []byte("measure#prefix.test=2.545ms")
	p := snatch.NewParser(30 * time.Second)

	bkts, err := p.Parse(m)

	assert.NoError(t, err)
	assert.Len(t, bkts, 1)
	assert.Equal(t, "prefix.test", bkts[0].ID.Name)
	assert.Equal(t, []float64{2.545}, bkts[0].Vals)
	assert.Equal(t, "ms", bkts[0].Units)
}

func TestParser_ParseHandlesTime(t *testing.T) {
	m := []byte("t=\"1983-02-21T01:23:45+0200\" lvl=info msg= count#test=2")
	p := snatch.NewParser(30 * time.Second)

	bkts, err := p.Parse(m)

	assert.NoError(t, err)
	assert.Len(t, bkts, 1)
	assert.Equal(t, int64(414631410), bkts[0].ID.Time.Unix())
}

func TestParser_ParseHandlesNoTime(t *testing.T) {
	m := []byte("lvl=info msg= count#test=2")
	p := snatch.NewParser(30 * time.Second)

	bkts, err := p.Parse(m)

	assert.NoError(t, err)
	assert.Len(t, bkts, 1)
	assert.Equal(t, time.Now().Truncate(time.Minute), bkts[0].ID.Time.Truncate(time.Minute))
}

func TestParser_ParseHandlesBadTime(t *testing.T) {
	m := []byte("t=\"1983-02-21T01:23:45\" lvl=info msg= count#test=2")
	p := snatch.NewParser(30 * time.Second)

	bkts, err := p.Parse(m)

	assert.NoError(t, err)
	assert.Len(t, bkts, 1)
	assert.Equal(t, time.Now().Truncate(time.Minute), bkts[0].ID.Time.Truncate(time.Minute))
}

func TestParser_ParseHandlesTags(t *testing.T) {
	m := []byte("t=\"1983-02-21T01:23:45+0200\" lvl=info msg= count#test=2 foo=\"bar\" size=10 test=test")
	p := snatch.NewParser(time.Second)

	bkts, err := p.Parse(m)

	want := []string{
		"foo", "bar",
		"size", "10",
		"test", "test",
	}
	assert.NoError(t, err)
	assert.Len(t, bkts, 1)
	assert.Equal(t, want, bkts[0].ID.Tags)
}

func TestParser_ParseHandlesRates(t *testing.T) {
	tests := []struct {
		metric []byte
		vals   []float64
	}{
		{
			metric: []byte("count#test@0.1=2"),
			vals:   []float64{20},
		},
		{
			metric: []byte("count#test@0.1=-2"),
			vals:   []float64{-20},
		},
		{
			metric: []byte("sample#test@0.1=2.3"),
			vals:   []float64{2.3},
		},
		{
			metric: []byte("measure#test@0.2=2.3ms"),
			vals:   []float64{2.3, 2.3, 2.3, 2.3, 2.3},
		},
	}

	for _, tt := range tests {
		p := snatch.NewParser(time.Second)

		bkts, err := p.Parse(tt.metric)

		assert.NoError(t, err)
		assert.Len(t, bkts, 1)
		assert.Equal(t, tt.vals, bkts[0].Vals)
	}
}

func BenchmarkParser_Parse(b *testing.B) {
	m := []byte("t=\"1983-02-21T01:23:45-0400\" lvl=info msg= count#test=2 foo=\"bar\" size=10")
	p := snatch.NewParser(time.Second)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Parse(m)
	}
}

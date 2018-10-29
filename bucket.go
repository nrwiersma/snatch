package snatch

import (
	"strings"
	"time"
)

// Type constants.
const (
	Count   Type = "count"
	Sample  Type = "sample"
	Measure Type = "measure"
)

// Type represents a metric type
type Type string

// ID represents a Buckets identity.
type ID struct {
	Time time.Time
	Name string
	Tags []string
	Type Type
}

// Keys returns the timestamp and key of an ID.
func (id *ID) Keys() (int64, string) {
	s := string(id.Type) + ":" + id.Name + ":" + strings.Join(id.Tags, ",")

	return id.Time.Unix(), s
}

// Bucket represents a metric bucket.
type Bucket struct {
	// ID is the Bucket identity.
	ID *ID
	// Units is the type of yhe values.
	Units string
	// Vals is the slice of values in the bucket.
	Vals []float64
	// Sum is the sum of the values.
	Sum float64
}

// Append adds a metric value to the bucket.
func (b *Bucket) Append(v float64) {
	b.Sum += v
	b.Vals = append(b.Vals, v)
}

// Merge merges a Bucket in to the current Bucket.
func (b *Bucket) Merge(v *Bucket) {
	for _, v := range v.Vals {
		b.Append(v)
	}
}

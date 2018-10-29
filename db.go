package snatch

import (
	"strings"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/nrwiersma/snatch/utils"
)

// DB represents
type DB interface {
	// Insert inserts the Buckets into the database.
	Insert([]*Bucket) error
	// Close closes the database.
	Close() error
}

type influxDB struct {
	c        client.Client
	database string
}

// NewDB creates a new InfluxDB instance.
func NewDB(c client.Client, database string) DB {
	return &influxDB{
		c:        c,
		database: database,
	}
}

// Insert inserts the Buckets into InfluxDB.
func (db *influxDB) Insert(bkts []*Bucket) error {
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  db.database,
		Precision: "s",
	})

	for _, bkt := range bkts {
		p, _ := client.NewPoint(
			db.formatName(bkt.ID.Name),
			db.formatTags(bkt.ID.Tags),
			db.formatValues(bkt),
			bkt.ID.Time,
		)

		bp.AddPoint(p)
	}

	return db.c.Write(bp)
}

func (db *influxDB) formatName(name string) string {
	return strings.Replace(name, ".", "_", -1)
}

func (db *influxDB) formatTags(tags []string) map[string]string {
	m := make(map[string]string, len(tags)/2)
	for i := 0; i < len(tags); i += 2 {
		m[tags[i]] = tags[i+1]
	}

	return m
}

func (db *influxDB) formatValues(b *Bucket) map[string]interface{} {
	v := map[string]interface{}{}

	switch b.ID.Type {
	case Count:
		v["value"] = b.Sum

	case Sample:
		v["value"] = b.Vals[len(b.Vals)-1]

	case Measure:
		v["90_percentile"] = utils.Percentile(b.Vals, 90)
		v["95_percentile"] = utils.Percentile(b.Vals, 95)
		v["97_percentile"] = utils.Percentile(b.Vals, 97)
		v["99_percentile"] = utils.Percentile(b.Vals, 99)
		v["count"] = len(b.Vals)
		v["lower"] = utils.Min(b.Vals)
		v["mean"] = b.Sum / float64(len(b.Vals))
		v["sum"] = b.Sum
		v["upper"] = utils.Max(b.Vals)
	}

	return v
}

// Close closes the database.
func (db *influxDB) Close() error {
	return db.c.Close()
}

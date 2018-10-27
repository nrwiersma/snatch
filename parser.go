package snatch

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/kr/logfmt"
)

var (
	timePrefix  = []byte{'t'}
	levelPrefix = []byte("lvl")
	msgPrefix   = []byte("msg")

	countPrefix   = "count"
	measurePrefix = "measure"
	samplePrefix  = "sample"

	measureSeparator = []byte{'#'}
	rateSeparator    = []byte{'@'}

	timeFormat = "2006-01-02T15:04:05-0700"
)

type tuples []*tuple

func (t *tuples) HandleLogfmt(k, v []byte) error {
	*t = append(*t, &tuple{k, v})
	return nil
}

type tuple struct {
	Key []byte
	Val []byte
}

func (t *tuple) Name() string {
	return string(t.Key)
}

func (t *tuple) String() string {
	return string(t.Val)
}

// Float64 splits the value into a float and its units.
func (t *tuple) Float64() (float64, string, error) {
	digits := make([]byte, 0)
	foundDecimal := false
	for i := range t.Val {
		b := t.Val[i]
		if b == '.' && !foundDecimal {
			foundDecimal = true
			digits = append(digits, b)
			continue
		}

		if b == '-' {
			digits = append(digits, b)
			continue
		}

		if b < '0' || b > '9' {
			break
		}

		digits = append(digits, b)
	}

	if len(digits) > 0 {
		units := string(t.Val[len(digits):])
		v, err := strconv.ParseFloat(string(digits), 10)
		if err != nil {
			return 0, "", err
		}

		return v, units, nil
	}

	return 0, "", errors.New("Unable to parse float")
}

type scanner struct {
	Tuples tuples
}

func (ld *scanner) Scan(d []byte) error {
	if err := logfmt.Unmarshal(d, &ld.Tuples); err != nil {
		return err
	}

	return nil
}

func (ld *scanner) Reset() {
	ld.Tuples = ld.Tuples[:0]
}

// Parser parses l2met metrics.
type Parser struct {
	s   *scanner
	res time.Duration
}

// NewParser creates a new Parser instance.
func NewParser(res time.Duration) *Parser {
	return &Parser{
		s:   &scanner{},
		res: res,
	}
}

// Parse parses an l2met line returning metric Buckets.
func (p *Parser) Parse(b []byte) ([]*Bucket, error) {
	p.s.Reset()
	if err := p.s.Scan(b); err != nil {
		return nil, fmt.Errorf("parser: error parsing line: %s", err)
	}

	ts := time.Now()
	var tags []string
	var bkts []*Bucket
	for _, t := range p.s.Tuples {
		if bytes.Equal(t.Key, timePrefix) {
			ts = p.parseTime(t)
			continue
		}

		if bytes.Equal(t.Key, levelPrefix) || bytes.Equal(t.Key, msgPrefix) {
			//Ignore level and msg
			continue
		}

		if bytes.Contains(t.Key, measureSeparator) {
			bkt, err := p.parseMetric(t)
			if err != nil {
				return nil, err
			}
			bkts = append(bkts, bkt)
			continue
		}

		tags = append(tags, t.Name(), t.String())
	}

	for _, bkt := range bkts {
		bkt.ID.Time = ts
		bkt.ID.Tags = tags
	}

	return bkts, nil
}

func (p *Parser) parseTime(t *tuple) time.Time {
	ts, err := time.Parse(timeFormat, t.String())
	if err != nil {
		ts = time.Now()
	}

	return time.Unix(0, int64((time.Duration(ts.UnixNano())/p.res)*p.res))
}

func (p *Parser) parseMetric(t *tuple) (*Bucket, error) {
	split := bytes.Split(t.Key, measureSeparator)
	if len(split) < 2 {
		return nil, errors.New("parser: error splitting '@'")
	}

	id := &ID{
		Type: string(split[0]),
	}

	if len(split[1]) == 0 {
		return nil, errors.New("parser: zero length name")
	}

	name := split[1]
	rate := float64(0)
	rateSplit := bytes.Split(split[1], rateSeparator)
	if len(rateSplit) > 1 {
		name = rateSplit[0]
		if f, err := strconv.ParseFloat(string(rateSplit[1][1:]), 64); err == nil {
			rate = f
		}
	}
	id.Name = string(name)

	bkt := &Bucket{
		ID: id,
	}

	v, units, err := t.Float64()
	if err != nil {
		return nil, errors.New("parser: invalid counter value: " + t.String())
	}
	bkt.Units = units

	switch id.Type {
	case countPrefix:
		if rate != 0 {
			v /= rate
		}
		bkt.Append(v)

	case samplePrefix:
		bkt.Append(v)

	case measurePrefix:
		if rate != 0 {
			for i := 0; i < int(1.0/rate); i++ {
				bkt.Append(v)
			}
		} else {
			bkt.Append(v)
		}

	default:
		return nil, errors.New("parser: invalid metric type: " + string(split[0]))
	}

	return bkt, nil
}

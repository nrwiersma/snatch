package snatch

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/kr/logfmt"
	"strconv"
	"time"
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
	pos := 0
	for i := range t.Val {
		if (t.Val[i] >= '0' && t.Val[i] <= '9') ||
			t.Val[i] == '.' || t.Val[i] == '-' {
			pos++
			continue
		}

		break
	}

	if pos > 0 {
		units := string(t.Val[pos:])
		v, err := strconv.ParseFloat(string(t.Val[:pos]), 10)
		if err != nil {
			return 0, "", err
		}

		return v, units, nil
	}

	return 0, "", errors.New("unable to parse float")
}

type scanner struct {
	Tuples tuples
}

func (s *scanner) Scan(b []byte) error {
	return logfmt.Unmarshal(b, &s.Tuples)
}

func (s *scanner) Reset() {
	s.Tuples = s.Tuples[:0]
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

	var ts time.Time
	tags := make([]string, 0, len(p.s.Tuples)*2)
	bkts := make([]*Bucket, 0 ,2)
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

	if ts.IsZero() {
		ts = time.Unix(0, int64((time.Duration(time.Now().UnixNano())/p.res)*p.res))
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
		return time.Time{}
	}

	return time.Unix(0, int64((time.Duration(ts.UnixNano())/p.res)*p.res))
}

func (p *Parser) parseMetric(t *tuple) (*Bucket, error) {
	split := bytes.SplitN(t.Key, measureSeparator, 2)
	id := &ID{
		Type: string(split[0]),
	}

	if len(split[1]) == 0 {
		return nil, errors.New("parser: zero length name")
	}

	name, rate := p.splitRate(split[1])
	id.Name = string(name)

	bkt := &Bucket{
		ID: id,
	}

	v, units, err := t.Float64()
	if err != nil {
		return nil, errors.New("parser: invalid float value: " + t.String())
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

func (p *Parser) splitRate(b []byte) ([]byte, float64) {
	if !bytes.Contains(b, rateSeparator) {
		return b, 0
	}

	split := bytes.SplitN(b, rateSeparator, 2)
	if f, err := strconv.ParseFloat(string(split[1]), 64); err == nil {
		return split[0], f
	}

	return split[0], 0
}

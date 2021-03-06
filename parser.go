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
	timeKey  = []byte{'t'}
	levelKey = []byte("lvl")
	msgKey   = []byte("msg")

	measureSeparator = []byte{'#'}
	rateSeparator    = []byte{'@'}
)

type tuples []*tuple

// HandleLogfmt implements the logfmt.Handler interface.
func (t *tuples) HandleLogfmt(k, v []byte) error {
	if bytes.Equal(k, timeKey) || bytes.Equal(k, levelKey) || bytes.Equal(k, msgKey) {
		return nil
	}

	*t = append(*t, &tuple{k, v})
	return nil
}

type tuple struct {
	Key []byte
	Val []byte
}

// Name returns the Key as a string.
func (t *tuple) Name() string {
	return string(t.Key)
}

// String returns the Val as a string.
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

	if pos == 0 {
		return 0, "", errors.New("unable to parse float")
	}

	units := string(t.Val[pos:])
	v, err := strconv.ParseFloat(string(t.Val[:pos]), 10)
	if err != nil {
		return 0, "", err
	}

	return v, units, nil

}

type scanner struct {
	Tuples tuples
}

// Scan scans for Tuples for the logfmt line.
func (s *scanner) Scan(b []byte) error {
	return logfmt.Unmarshal(b, &s.Tuples)
}

// Reset clears the scanner.
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
	bkts := make([]*Bucket, 0, 2)
	for _, t := range p.s.Tuples {
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

	ts = time.Now().Truncate(p.res)
	for _, bkt := range bkts {
		bkt.ID.Time = ts
		bkt.ID.Tags = tags
	}

	return bkts, nil
}

func (p *Parser) parseMetric(t *tuple) (*Bucket, error) {
	split := bytes.SplitN(t.Key, measureSeparator, 2)
	id := &ID{
		Type: Type(split[0]),
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
	case Count:
		if rate != 0 {
			v /= rate
		}
		bkt.Append(v)

	case Sample:
		bkt.Append(v)

	case Measure:
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
	i := bytes.Index(b, rateSeparator)
	if i < 0 {
		return b, 0
	}

	f, _ := strconv.ParseFloat(string(b[i+1:]), 64)
	return b[:i], f
}

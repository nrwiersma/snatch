package snatch

import (
	"bufio"
	"io"
	"time"
)

// Application is the application context.
type Application struct {
	db DB
	p  *Parser
	s  Store
}

// NewApplication creates a new Application.
func NewApplication(res time.Duration, db DB, s Store) *Application {
	return &Application{
		db: db,
		p:  NewParser(res),
		s:  s,
	}
}

// Parse parses lines from the Reader, adding them to the Store.
func (a *Application) Parse(r io.Reader, fn func([]byte)) error {
	rd := bufio.NewReader(r)

	for {
		line, err := rd.ReadBytes('\n')
		if len(line) > 0 {
			bkts, err := a.p.Parse(line)
			if err != nil || len(bkts) == 0 {
				fn(line)
				continue
			}

			if err := a.s.Add(bkts...); err != nil {
				return err
			}
		}

		if err != nil {
			if err == io.EOF {
				return nil
			}

			return err
		}
	}
}

// Scan inserts complete Buckets into the database.
func (a *Application) Scan() error {
	out, err := a.s.Scan()
	if err != nil {
		return err
	}

	var bkts []*Bucket
	for bkt := range out {
		bkts = append(bkts, bkt)
	}

	return a.db.Insert(bkts)
}

// Flush inserts all Buckets into the database.
func (a *Application) Flush() error {
	out, err := a.s.Flush()
	if err != nil {
		return err
	}

	var bkts []*Bucket
	for bkt := range out {
		bkts = append(bkts, bkt)
	}

	return a.db.Insert(bkts)
}

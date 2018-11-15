package snatch

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sync"
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

type ParseOpts struct {
	BufferSize     int
	AllowedPending int
}

var parserPool = sync.Pool{New: func() interface{} { return &bytes.Buffer{} }}

// Parse parses lines from the Reader, adding them to the Store.
func (a *Application) Parse(r io.Reader, opts ParseOpts, errFn func([]byte)) error {
	drops := 0
	wg := sync.WaitGroup{}
	in := make(chan *bytes.Buffer, opts.AllowedPending)
	buf := parserPool.Get().(*bytes.Buffer)

	wg.Add(1)
	go a.parseBuffers(in, &wg, errFn)

	rd := bufio.NewReader(r)
	for {
		b, err := rd.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		buf.Write(b)

		if buf.Len() < opts.BufferSize {
			continue
		}

		// Swap buffers
		select {
		case in <- buf:
		default:
			parserPool.Put(buf)
			drops++
			if drops == 1 || opts.AllowedPending == 0 || drops%opts.AllowedPending == 0 {
				fmt.Printf("snatch: message queue full. Dropped %d messages so far.\n", drops)

			}
		}

		buf = parserPool.Get().(*bytes.Buffer)
	}

	if buf.Len() > 0 {
		in <- buf
	}

	close(in)
	wg.Wait()

	return nil
}

func (a *Application) parseBuffers(in chan *bytes.Buffer, wg *sync.WaitGroup, errFn func([]byte)) {
	defer wg.Done()

	for buf := range in {
		for {
			b, err := buf.ReadBytes('\n')
			if len(b) > 0 {
				bkts, err := a.p.Parse(b)
				if err != nil || len(bkts) == 0 {
					errFn(b)
					continue
				}

				_ = a.s.Add(bkts...)
			}

			if err != nil {
				break
			}
		}

		parserPool.Put(buf)
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

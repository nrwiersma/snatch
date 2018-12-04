package snatch

import (
	"sync"
	"time"
)

// Store represents a Bucket store.
type Store interface {
	// Add adds Buckets into the Store.
	Add(...*Bucket) error
	// Scan scans the store for complete Buckets.
	Scan() (<-chan *Bucket, error)
	// Flush flushes all Buckets from the Store.
	Flush() (<-chan *Bucket, error)
}

type memStore struct {
	sync.Mutex

	res time.Duration

	store map[int64]map[string]*Bucket
}

// NewStore creates a new in-memory store.
func NewStore(res time.Duration) Store {
	return &memStore{
		res:   res,
		store: map[int64]map[string]*Bucket{},
	}
}

// Add adds Buckets into the Store.
func (s *memStore) Add(bkts ...*Bucket) error {
	s.Lock()

	for _, bkt := range bkts {
		ts, key := bkt.ID.Keys()

		box, ok := s.store[ts]
		if !ok {
			s.store[ts] = map[string]*Bucket{
				key: bkt,
			}
			continue
		}

		if b, ok := box[key]; ok {
			b.Merge(bkt)
			continue
		}

		box[key] = bkt
	}

	s.Unlock()

	return nil
}

// Scan scans the store for complete Buckets.
func (s *memStore) Scan() (<-chan *Bucket, error) {
	s.Lock()

	buckets := make(chan *Bucket, 1000)
	go func(out chan *Bucket) {
		ready := time.Now().Truncate(s.res).Add(-1 * (s.res + time.Second)).Unix()

		for ts, box := range s.store {
			if ts >= ready {
				continue
			}

			for _, v := range box {
				out <- v
			}
			delete(s.store, ts)
		}

		close(out)

		s.Unlock()
	}(buckets)

	return buckets, nil
}

// Flush flushes all Buckets from the Store.
func (s *memStore) Flush() (<-chan *Bucket, error) {
	s.Lock()

	buckets := make(chan *Bucket, 1000)
	go func(out chan *Bucket) {
		for ts, box := range s.store {
			for _, v := range box {
				out <- v
			}
			delete(s.store, ts)
		}

		close(out)

		s.Unlock()
	}(buckets)

	return buckets, nil
}

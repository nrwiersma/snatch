package snatch_test

import (
	"github.com/nrwiersma/snatch"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/stretchr/testify/mock"
)

type mockDB struct {
	mock.Mock
}

func (m *mockDB) Insert(bkts []*snatch.Bucket) error {
	args := m.Called(bkts)
	return args.Error(0)
}

func (m *mockDB) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockStore struct {
	mock.Mock
}

func (m *mockStore) Add(bkts ...*snatch.Bucket) error {
	args := m.Called(bkts)
	return args.Error(0)
}

func (m *mockStore) Scan() (out <-chan *snatch.Bucket, err error) {
	args := m.Called()
	return args.Get(0).(chan *snatch.Bucket), args.Error(1)
}

func (m *mockStore) Flush() (<-chan *snatch.Bucket, error) {
	args := m.Called()
	return args.Get(0).(chan *snatch.Bucket), args.Error(1)
}

type mockClient struct {
	mock.Mock
}

func (m *mockClient) Ping(timeout time.Duration) (time.Duration, string, error) {
	args := m.Called(timeout)
	return args.Get(0).(time.Duration), args.String(1), args.Error(2)
}

func (m *mockClient) Write(bp client.BatchPoints) error {
	args := m.Called(bp)
	return args.Error(0)
}

func (m *mockClient) Query(q client.Query) (*client.Response, error) {
	args := m.Called(q)
	return args.Get(0).(*client.Response), args.Error(1)
}

func (m *mockClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

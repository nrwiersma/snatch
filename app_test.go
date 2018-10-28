package snatch_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/nrwiersma/snatch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewApplication(t *testing.T) {
	db := new(mockDB)
	s := new(mockStore)

	app := snatch.NewApplication(10*time.Second, db, s)

	assert.IsType(t, &snatch.Application{}, app)
}

func TestApplication_Parse(t *testing.T) {
	b := []byte(`test
t="1983-02-21T01:23:45-0400" lvl=info msg= count#test=2 foo="bar" size=10
t="1983-02-21T01:23:45-0400" lvl=info msg= count#test=2 foo="bar" size=10`)

	db := new(mockDB)
	s := new(mockStore)
	s.On("Add", mock.Anything).Return(nil)
	app := snatch.NewApplication(10*time.Second, db, s)

	err := app.Parse(bytes.NewReader(b), func(b []byte) {
		assert.Equal(t, []byte("test\n"), b)
	})

	assert.NoError(t, err)
}

func TestApplication_ParseStoreError(t *testing.T) {
	b := []byte(`t="1983-02-21T01:23:45-0400" lvl=info msg= count#test=2 foo="bar" size=10`)

	db := new(mockDB)
	s := new(mockStore)
	s.On("Add", mock.Anything).Return(errors.New("test"))
	app := snatch.NewApplication(10*time.Second, db, s)

	err := app.Parse(bytes.NewReader(b), func(b []byte) {})

	assert.Error(t, err)
}

func TestApplication_Scan(t *testing.T) {
	out := make(chan *snatch.Bucket, 1)
	out <- &snatch.Bucket{}
	close(out)

	db := new(mockDB)
	db.On("Insert", mock.Anything).Return(nil)
	s := new(mockStore)
	s.On("Scan").Return(out, nil)
	app := snatch.NewApplication(10*time.Second, db, s)

	err := app.Scan()

	assert.NoError(t, err)
}

func TestApplication_ScanError(t *testing.T) {
	out := make(chan *snatch.Bucket)

	db := new(mockDB)
	db.On("Insert", mock.Anything).Return(nil)
	s := new(mockStore)
	s.On("Scan").Return(out, errors.New("test"))
	app := snatch.NewApplication(10*time.Second, db, s)

	err := app.Scan()

	assert.Error(t, err)
}

func TestApplication_Flush(t *testing.T) {
	out := make(chan *snatch.Bucket, 1)
	out <- &snatch.Bucket{}
	close(out)

	db := new(mockDB)
	db.On("Insert", mock.Anything).Return(nil)
	s := new(mockStore)
	s.On("Flush").Return(out, nil)
	app := snatch.NewApplication(10*time.Second, db, s)

	err := app.Flush()

	assert.NoError(t, err)
}

func TestApplication_FlushError(t *testing.T) {
	out := make(chan *snatch.Bucket)

	db := new(mockDB)
	db.On("Insert", mock.Anything).Return(nil)
	s := new(mockStore)
	s.On("Flush").Return(out, errors.New("test"))
	app := snatch.NewApplication(10*time.Second, db, s)

	err := app.Flush()

	assert.Error(t, err)
}

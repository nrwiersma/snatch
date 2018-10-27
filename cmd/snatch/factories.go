package main

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/nrwiersma/snatch"
)

// DB ======================================

func newDB(dsn string) (snatch.DB, error) {
	if dsn == "" {
		return nil, fmt.Errorf("invalid db: %s", dsn)
	}

	uri, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	password, _ := uri.User.Password()
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     uri.Scheme + "://" + uri.Host,
		Username: uri.User.Username(),
		Password: password,
	})
	if err != nil {
		return nil, err
	}

	db := strings.Trim(uri.Path, "/")

	return snatch.NewDB(c, db), nil
}

// Application =============================

func newApplication(res time.Duration, db snatch.DB, s snatch.Store) *snatch.Application {
	return snatch.NewApplication(res, db, s)
}

// Store ===================================

func newStore(res time.Duration) snatch.Store {
	return snatch.NewStore(res)
}

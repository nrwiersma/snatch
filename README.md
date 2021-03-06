![Logo](http://svg.wiersma.co.za/github/project?lang=go&title=snatch&tag=l2met%20parser)

[![Go Report Card](https://goreportcard.com/badge/github.com/nrwiersma/snatch)](https://goreportcard.com/report/github.com/nrwiersma/snatch)
[![Build Status](https://travis-ci.org/nrwiersma/snatch.svg?branch=master)](https://travis-ci.org/nrwiersma/snatch)
[![Coverage Status](https://coveralls.io/repos/github/nrwiersma/snatch/badge.svg?branch=master)](https://coveralls.io/github/nrwiersma/snatch?branch=master)
[![GitHub release](https://img.shields.io/github/release/nrwiersma/snatch.svg)](https://github.com/nrwiersma/snatch/releases)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/nrwiersma/snatch/master/LICENSE)

## About

Snatch is a l2met parser that inserts the data into InfluxDB. If it cannot parse the line, it outputs
back to stdout.

## Installation

Download the [binary](https://github.com/nrwiersma/snatch/releases) or

```bash
$ go get github.com/nrwiersma/snatch/cmd/snatch
```

## Usage

Snatch parse metrics in `logfmt` lines from `stdin` in the format
```
lvl=info msg= count#test=2 foo="bar" size=10
```

The time is optional, defaulting to now, and the `lvl` and `msg` will be ignored in the metrics.
All other non-metric pieces will be used as tags in the metric.

While not standard, snatch handles sampling. You can add the sample rate at the end of the
name separated by an `@`

```
lvl=info msg= count#test@0.1=2 foo="bar" size=10
``` 

Snatch requires the `--db` flag with the DSN of InfluxDB in the format

```bash
$ snatch --db=http://localhost:8086/database
```

optionally you can set the resolution of the buckets (default is `10s`)

```bash
$ snatch --db=http://localhost:8086/database --res=30s
```

Setting these options can be tedious, so a YAML config file can be used (default path is `~/.snatch.yaml`)

```bash
$ snatch --config=testdata/config.yaml
```

which is in the form

```yaml
db: http://localhost:8086/metrics
res: 30s
```
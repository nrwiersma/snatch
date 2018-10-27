include github.com/msales/make/golang

# Print test data to stdout
testdata:
	@cat testdata/metrics.txt | sed s/yyyymmdd/`date +%Y-%m-%dT%H:%M:%S%z`/
.PHONY: testdata
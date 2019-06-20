.PHONY: all
all:
	go build -v gitlab.wsq.io/beacon-ext/dvara/cmd/dvara

.PHONY: bootstrap
bootstrap:

.PHONY: test
test:
	if [ `go fmt $(go list ./... | grep -v /vendor/) | wc -l` -gt 0 ]; then echo "go fmt error"; exit 1; fi

#*************** Release ***************
pre_release:
	wst-go-pkg-rel

release:
	wst-go-pkg-rel --release

test_release:
	wst-go-pkg-rel --dry-run

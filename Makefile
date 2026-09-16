gtrace_sources := $(shell find . -name '*_trace.go' -not -name '*_gtrace.go')
gtrace_targets := $(patsubst %_trace.go,%_trace_gtrace.go,$(gtrace_sources))

all: generate

_bin:
	mkdir -p _bin

# gtrace is a generator only: nothing in the module imports its runtime, so
# it installs by version, independent of go.mod.
gtrace_version := v0.6.0

# gtrace type-checks with the go/types it was built with, so a binary from an
# older Go cannot read a newer standard library. The stamp names both
# versions: changing either makes a new stamp, which rebuilds the binary.
go_version := $(shell go env GOVERSION)
gtrace_stamp := _bin/.gtrace-$(gtrace_version)-$(go_version)

$(gtrace_stamp): | _bin
	rm -f _bin/.gtrace-*
	touch $@

# go install leaves an up-to-date binary alone, so touch it to keep it newer
# than the stamp.
_bin/gtrace: $(gtrace_stamp)
	GOBIN=$(PWD)/_bin go install github.com/gobwas/gtrace/cmd/gtrace@$(gtrace_version)
	touch $@

$(gtrace_targets): %_trace_gtrace.go: %_trace.go _bin/gtrace
	$(eval changed = $(filter %.go,$?))
	$(if $(changed), PATH=$(PWD)/_bin:$(PATH) go generate -run gtrace $(changed))

.PHONY: generate_gtrace
generate_gtrace: $(gtrace_targets)
.PHONY: generate
generate: generate_gtrace

.PHONY: build
build: generate
	go build ./...

.PHONY: test
test: generate
	go vet ./...
	go test ./... -cover

.PHONY: clean
clean:
	rm -f _bin/gtrace _bin/.gtrace-*
	find . -name "*_gtrace.go" -delete

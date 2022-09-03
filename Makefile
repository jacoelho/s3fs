# disable default rules
.SUFFIXES:
MAKEFLAGS+=-r -R
GOBIN = $(shell go env GOPATH)/bin
DATE  = $(shell date +%Y%m%d%H%M%S)

.PHONY: test
test:
	cd tests; \
 	go test -race -shuffle=on -v ./...

.PHONY: test-short
test-short:
	cd tests; \
 	go test -race -shuffle=on -short -v ./...

.PHONY: ci-tidy
ci-tidy:
	go mod tidy
	git status --porcelain go.mod go.sum || { echo "Please run 'go mod tidy'."; exit 1; }

$(GOBIN)/staticcheck:
	go install honnef.co/go/tools/cmd/staticcheck@latest

.PHONY: staticcheck
staticcheck: $(GOBIN)/staticcheck
	$(GOBIN)/staticcheck ./...

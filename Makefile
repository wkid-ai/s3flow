.PHONY: build test vet ui-test
build:
	go build -trimpath -o bin/s3flow ./cmd/s3flow
vet:
	go vet ./...
test: ui-test
	go test -race -count=1 ./...
ui-test:
	node --test internal/ui/lib.test.js

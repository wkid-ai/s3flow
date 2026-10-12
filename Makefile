.PHONY: build test vet
build:
	go build -trimpath -o bin/s3qos ./cmd/s3qos
vet:
	go vet ./...
test:
	go test -race -count=1 ./...

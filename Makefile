.PHONY: build test test-race vet run clean

build:
	@mkdir -p bin
	go build -o bin/larm ./cmd/larm

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

run:
	go run ./cmd/larm -config "$${LARM_CONFIG:-configs/models.yaml}"

clean:
	rm -f bin/larm

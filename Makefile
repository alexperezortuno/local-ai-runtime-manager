.PHONY: build test test-race test-real-crash vet run clean

build:
	@mkdir -p bin
	go build -o bin/larm ./cmd/larm

test:
	go test ./...

test-race:
	go test -race ./...

test-real-crash:
	LARM_REAL_CRASH=1 go test ./internal/recovery -run TestRealSIGKILLCrashRecovery -count=1 -v

vet:
	go vet ./...

run:
	go run ./cmd/larm -config "$${LARM_CONFIG:-configs/models.yaml}"

clean:
	rm -f bin/larm

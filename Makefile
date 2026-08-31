.PHONY: fmt lint test race integration build benchmark smoke

fmt:
	test -z "$$(gofmt -l .)"

lint:
	golangci-lint run

test:
	go test ./... -count=1

race:
	go test -race ./... -count=1

integration:
	STREAMFORGE_INTEGRATION=1 go test ./tests/integration -count=1

build:
	go build ./cmd/streamforge

benchmark:
	go test ./internal/processing -bench . -benchmem -count=1

smoke:
	docker compose up --build --abort-on-container-exit --exit-code-from api

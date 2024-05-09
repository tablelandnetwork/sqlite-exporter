# Lint
lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.54.2 run
.PHONY: lint

# Test
test: 
	go test ./... -short -race -timeout 1m
.PHONY: test

# Build
build:
	go build -o sqlite-exporter .
.PHONY: build
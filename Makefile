default: clean test build
clean:
	rm -rf build
build:
	go build -o build/nf-shard-worker ./cmd
run:
	go run ./cmd/main.go
gqlgen:
	go run github.com/99designs/gqlgen generate
test:
	go test ./...
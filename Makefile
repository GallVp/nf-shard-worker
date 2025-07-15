build:
	go build -o build/nf-shard-worker ./cmd
run:
	go run ./cmd/main.go
gqlgen:
	go run github.com/99designs/gqlgen generate
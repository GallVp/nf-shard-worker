build:
	go build -o main ./cmd
run:
	go run ./cmd/main.go
gqlgen:
	go run github.com/99designs/gqlgen generate
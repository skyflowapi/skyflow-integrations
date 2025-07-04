.PHONY: format test coverage lint

format:
	gofmt -w .

test:
	go test ./...

coverage:
	go test ./... -covermode=count -coverprofile=cover.out && go tool cover -html=cover.out

lint:
	golangci-lint run

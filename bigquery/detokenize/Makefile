GCP_PROJECT_ID := ""
IMAGE_NAME := detokenize
IMAGE_TAG := $(shell date +%Y%m%d%H%M)

.PHONY: guard-% gcloud-build run format test coverage lint

guard-%:
	@if [ "${${*}}" = "" ]; then \
		echo "Variable $* not set"; \
    exit 1; \
  fi

gcloud-build: guard-GCP_PROJECT_ID guard-IMAGE_NAME guard-IMAGE_TAG
	gcloud builds submit --tag "gcr.io/$(GCP_PROJECT_ID)/$(IMAGE_NAME):$(IMAGE_TAG)" .

run:
	go run ./cmd/main.go

format:
	gofmt -w .

test:
	go test ./...

coverage:
	go test ./... -covermode=count -coverprofile=cover.out && go tool cover -html=cover.out

lint:
	golangci-lint run

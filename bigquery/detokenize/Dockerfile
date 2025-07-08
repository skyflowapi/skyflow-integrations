# Copyright (c) 2025 Skyflow, Inc.

# Use the official Golang image as a build stage
FROM golang:1.24-alpine3.20 AS build

# Set the working directory inside the container
WORKDIR /app

# Copy the Go modules files
COPY go.mod go.sum ./

# Download git
RUN apk add --no-cache git

# Download and cache the Go modules
RUN go mod download

# Copy the rest of the application source code
COPY . .

# Build the Go application from cmd/main.go
RUN go build -o main ./cmd/main.go

# Use a minimal base image to package the built binary
FROM alpine:3.22

# Set the working directory inside the container
WORKDIR /root/

# Copy the binary from the build stage
COPY --from=build /app/main .

# Expose the port the application runs on
EXPOSE 8080

# Command to run the application
CMD ["./main"]

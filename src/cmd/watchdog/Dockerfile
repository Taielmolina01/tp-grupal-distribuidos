FROM golang:1.25-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/watchdog ./internal/watchdog
COPY cmd/watchdog ./cmd/watchdog
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/watchdog ./cmd/watchdog/

FROM alpine:3.23
RUN apk add --no-cache docker-cli
COPY --from=builder /build/bin/watchdog /watchdog
ENTRYPOINT ["./watchdog"]

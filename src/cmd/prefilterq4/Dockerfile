FROM golang:1.25-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/prefilterq4 ./internal/prefilterq4
COPY cmd/prefilterq4 ./cmd/prefilterq4
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/prefilterq4 ./cmd/prefilterq4/

FROM busybox:latest
COPY --from=builder /build/bin/prefilterq4 /prefilterq4
ENTRYPOINT ["./prefilterq4"]

FROM golang:1.25-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/join ./internal/join
COPY cmd/join ./cmd/join
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/join ./cmd/join/

FROM busybox:latest
COPY --from=builder /build/bin/join /join
ENTRYPOINT ["./join"]

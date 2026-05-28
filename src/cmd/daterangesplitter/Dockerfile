FROM golang:1.25-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/daterangesplitter ./internal/daterangesplitter
COPY cmd/daterangesplitter ./cmd/daterangesplitter
RUN go mod download
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/daterangesplitter ./cmd/daterangesplitter/

FROM busybox:latest
COPY --from=builder /build/bin/daterangesplitter /daterangesplitter
ENTRYPOINT ["./daterangesplitter"]

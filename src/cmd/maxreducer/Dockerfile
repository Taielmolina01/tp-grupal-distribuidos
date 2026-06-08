FROM golang:1.25-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/maxreducer ./internal/maxreducer
COPY cmd/maxreducer ./cmd/maxreducer
RUN go mod download
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/maxreducer ./cmd/maxreducer/

FROM busybox:latest
COPY --from=builder /build/bin/maxreducer /maxreducer
ENTRYPOINT ["./maxreducer"]

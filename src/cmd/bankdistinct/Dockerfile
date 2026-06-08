FROM golang:1.25-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/bankdistinct ./internal/bankdistinct
COPY cmd/bankdistinct ./cmd/bankdistinct
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/bankdistinct ./cmd/bankdistinct/

FROM busybox:latest
COPY --from=builder /build/bin/bankdistinct /bankdistinct
ENTRYPOINT ["./bankdistinct"]

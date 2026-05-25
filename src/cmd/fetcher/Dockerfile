FROM golang:1.24-alpine3.23 AS builder
# Client uses docker multistage builds feature https://docs.docker.com/develop/develop-images/multistage-build/
LABEL intermediateStageToBeDeleted=true
RUN apk add --no-cache ca-certificates curl && update-ca-certificates
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/fetcher ./internal/fetcher
COPY cmd/fetcher ./cmd/fetcher
RUN go mod download
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/fetcher ./cmd/fetcher/

FROM busybox:latest
COPY --from=builder /build/bin/fetcher /fetcher
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENTRYPOINT ["./fetcher"]

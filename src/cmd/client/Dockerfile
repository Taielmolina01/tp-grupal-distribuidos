FROM golang:1.24-alpine3.23 AS builder
# Client uses docker multistage builds feature https://docs.docker.com/develop/develop-images/multistage-build/
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY internal/common ./internal/common
COPY internal/client ./internal/client
COPY cmd/client ./cmd/client
RUN go mod download
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/client ./cmd/client/

FROM busybox:latest
COPY --from=builder /build/bin/client /client
ENTRYPOINT ["./client"]

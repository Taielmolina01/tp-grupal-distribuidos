FROM golang:1.24-alpine3.23 AS builder
# Client uses docker multistage builds feature https://docs.docker.com/develop/develop-images/multistage-build/
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/filter ./internal/filter
COPY cmd/filter ./cmd/filter
RUN go mod download
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/filter ./cmd/filter/

FROM busybox:latest
COPY --from=builder /build/bin/filter /filter
ENTRYPOINT ["./filter"]

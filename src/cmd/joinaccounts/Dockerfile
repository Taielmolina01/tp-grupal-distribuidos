FROM golang:1.24-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/filter/joinaccounts ./internal/filter/joinaccounts
COPY cmd/joinaccounts ./cmd/joinaccounts
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/joinaccounts ./cmd/joinaccounts/

FROM busybox:latest
COPY --from=builder /build/bin/joinaccounts /joinaccounts
ENTRYPOINT ["./joinaccounts"]

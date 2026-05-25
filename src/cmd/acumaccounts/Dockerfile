FROM golang:1.24-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/acumaccounts ./internal/acumaccounts
COPY cmd/acumaccounts ./cmd/acumaccounts
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/acumaccounts ./cmd/acumaccounts/

FROM busybox:latest
COPY --from=builder /build/bin/acumaccounts /acumaccounts
ENTRYPOINT ["./acumaccounts"]

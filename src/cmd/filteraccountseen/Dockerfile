FROM golang:1.25-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/filter/filteraccountseen ./internal/filter/filteraccountseen
COPY cmd/filteraccountseen ./cmd/filteraccountseen
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/filteraccountseen ./cmd/filteraccountseen/

FROM busybox:latest
COPY --from=builder /build/bin/filteraccountseen /filteraccountseen
ENTRYPOINT ["./filteraccountseen"]

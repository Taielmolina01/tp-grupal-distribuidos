FROM golang:1.25-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/filter/filtercurrency ./internal/filter/filtercurrency
COPY cmd/filtercurrency ./cmd/filtercurrency
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/filtercurrency ./cmd/filtercurrency/

FROM busybox:latest
COPY --from=builder /build/bin/filtercurrency /filtercurrency
ENTRYPOINT ["./filtercurrency"]

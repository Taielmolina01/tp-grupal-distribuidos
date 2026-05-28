FROM golang:1.25-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/averagefilter ./internal/averagefilter
COPY cmd/averagefilter ./cmd/averagefilter
RUN go mod download
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/averagefilter ./cmd/averagefilter/

FROM busybox:latest
COPY --from=builder /build/bin/averagefilter /averagefilter
ENTRYPOINT ["./averagefilter"]

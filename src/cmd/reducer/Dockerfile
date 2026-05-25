FROM golang:1.25-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/reducer ./internal/reducer
COPY cmd/reducer ./cmd/reducer
RUN go mod download
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/reducer ./cmd/reducer/

FROM busybox:latest
COPY --from=builder /build/bin/reducer /reducer
ENTRYPOINT ["./reducer"]

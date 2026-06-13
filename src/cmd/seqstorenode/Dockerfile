FROM golang:1.25-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common/seqstore ./internal/common/seqstore
COPY internal/common/middleware/newmiddleware ./internal/common/middleware/newmiddleware
COPY internal/seqstorenode ./internal/seqstorenode
COPY cmd/seqstorenode ./cmd/seqstorenode
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/seqstorenode ./cmd/seqstorenode/

FROM busybox:latest
COPY --from=builder /build/bin/seqstorenode /seqstorenode
ENTRYPOINT ["./seqstorenode"]

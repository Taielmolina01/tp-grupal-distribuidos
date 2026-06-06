FROM golang:1.25-alpine3.23 AS builder
# Client uses docker multistage builds feature https://docs.docker.com/develop/develop-images/multistage-build/
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY internal/common ./internal/common
COPY internal/filter/daterangeandpaymentfilter ./internal/filter/daterangeandpaymentfilter
COPY internal/filter/commonfilter ./internal/filter/commonfilter
COPY cmd/daterangeandpaymentfilter ./cmd/daterangeandpaymentfilter
RUN go mod download
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/daterangeandpaymentfilter ./cmd/daterangeandpaymentfilter/

FROM busybox:latest
COPY --from=builder /build/bin/daterangeandpaymentfilter /daterangeandpaymentfilter
ENTRYPOINT ["./daterangeandpaymentfilter"]

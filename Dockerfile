FROM golang:1.22-alpine3.19 AS base

WORKDIR /github.com/timescale/csv-importer

COPY ./go.mod go.mod
COPY ./go.sum go.sum
RUN go mod download

FROM base as builder
COPY ./cmd cmd
COPY ./internal internal

RUN go build -o /bin/timescaledb-parallel-copy ./cmd/timescaledb-parallel-copy

FROM alpine:3.19 as release

COPY --from=builder /bin/timescaledb-parallel-copy /bin/timescaledb-parallel-copy

ENTRYPOINT [ "timescaledb-parallel-copy" ]

FROM golang:1.19-alpine3.15 AS build 

WORKDIR  /stage 

COPY . . 

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /stage/timescaledb-parallel-copy ./cmd/... 

FROM scratch 

WORKDIR /data

COPY --from=build /stage/timescaledb-parallel-copy /usr/bin/

ENTRYPOINT [ "timescaledb-parallel-copy" ]


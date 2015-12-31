FROM golang:latest

WORKDIR /go/src/github.com/noxiouz/docker-distribution-postgresql

COPY . ./

RUN go get -t ./...

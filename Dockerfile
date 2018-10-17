FROM golang:1.10.3
RUN mkdir -p /go/bin
ADD ./bin/tau_server-linux-amd64 /go/bin/tau_server
WORKDIR /go/bin
ENTRYPOINT ["./tau_server"]

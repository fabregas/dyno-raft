FROM golang:1.7

WORKDIR $GOPATH

RUN go get github.com/hashicorp/raft && go get github.com/hashicorp/raft-boltdb

ADD http $GOPATH/src/dyno-raft/http
ADD store $GOPATH/src/dyno-raft/store
ADD main.go $GOPATH/src/dyno-raft/

RUN go install dyno-raft

VOLUME /data

ENTRYPOINT ["dyno-raft", "-haddr", "_eth0_:11000", "-raddr", "_eth0_:12000", "-dir", "/data"]


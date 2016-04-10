all: $(patsubst %.proto,%.pb.go,$(wildcard proto/*.proto))

clean:
	rm -f proto/*.pb.go
	rm -f raft-demo-server raft-discover

gofmt:
	gofmt -w $(wildcard *.go */*.go)

%.pb.go: %.proto
	protoc --go_out=. $<

.PHONY: all clean gofmt

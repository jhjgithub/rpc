
TARGET=client server

all: $(TARGET)

.PHONY: server
server:
	cd cmd/server; go build;

.PHONY: client
client:
	cd cmd/client; go build;


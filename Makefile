
TARGET=client server biclient

all: $(TARGET)

.PHONY: server
server:
	cd cmd/server; go build;

.PHONY: client
client:
	cd cmd/client; go build;

.PHONY: biclient
biclient:
	cd cmd/biclient; go build;


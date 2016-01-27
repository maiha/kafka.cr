LINK_FLAGS=--link-flags "-static"

all: build

build: kafka-heartbeat kafka-ping

kafka-heartbeat: src/main/heartbeat.cr
	@mkdir -p bin
	crystal build --release src/main/heartbeat.cr -o bin/kafka-heartbeat ${LINK_FLAGS}

kafka-ping: src/main/ping.cr
	@mkdir -p bin
	crystal build --release src/main/ping.cr -o bin/kafka-ping ${LINK_FLAGS}

clean:
	@rm -rf bin

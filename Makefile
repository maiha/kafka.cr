LINK_FLAGS=--link-flags "-static"

all: build

build: kafka-heartbeat kafka-ping

kafka-heartbeat: src/bin/heartbeat.cr
	@mkdir -p bin
	crystal build --release src/bin/heartbeat.cr -o bin/kafka-heartbeat ${LINK_FLAGS}

kafka-ping: src/bin/ping.cr
	@mkdir -p bin
	crystal build --release src/bin/ping.cr -o bin/kafka-ping ${LINK_FLAGS}

clean:
	@rm -rf bin

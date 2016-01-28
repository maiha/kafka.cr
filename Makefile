LINK_FLAGS = --link-flags "-static"
BIN_SRCS = src/bin/*.cr

.PHONY : all build clean test spec test-compile-bin bin
.PHONY : kafka-heartbeat kafka-ping

all: build

build: bin kafka-heartbeat kafka-ping

bin:
	@mkdir -p bin

kafka-heartbeat: src/bin/heartbeat.cr
	crystal build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-ping: src/bin/ping.cr
	crystal build --release $^ -o bin/$@ ${LINK_FLAGS}

test: test-compile-bin spec

spec:
	crystal spec -v

test-compile-bin:
	crystal build src/bin/heartbeat.cr -o /dev/null
	crystal build src/bin/ping.cr -o /dev/null

clean:
	@rm -rf bin tmp

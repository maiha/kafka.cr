LINK_FLAGS = --link-flags "-static"
BIN_SRCS = src/bin/*.cr

.PHONY : all build clean test spec test-compile-bin bin
.PHONY : kafka-heartbeat kafka-metadata kafka-ping

all: build

build: bin kafka-heartbeat kafka-metadata kafka-ping

bin:
	@mkdir -p bin

kafka-heartbeat: src/bin/heartbeat.cr
	crystal build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-metadata: src/bin/metadata.cr
	crystal build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-ping: src/bin/ping.cr
	crystal build --release $^ -o bin/$@ ${LINK_FLAGS}

test: test-compile-bin spec

spec:
	crystal spec -v

test-compile-bin:
	@for x in src/bin/*.cr ; do\
	  crystal build "$$x" -o /dev/null ;\
	done

clean:
	@rm -rf bin tmp

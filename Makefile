SHELL = /bin/bash
LINK_FLAGS = --link-flags "-static"
SRCS = ${wildcard src/bin/*.cr}
PROGS = $(SRCS:src/bin/%.cr=kafka-%)

.PHONY : all build clean test spec test-compile-bin bin
.PHONY : ${PROGS}

all: build

build: bin ${PROGS}

bin:
	@mkdir -p bin

#kafka-%: src/bin/%.cr
#	crystal build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-error: src/bin/error.cr
	crystal build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-fetch: src/bin/fetch.cr
	crystal build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-heartbeat: src/bin/heartbeat.cr
	crystal build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-metadata: src/bin/metadata.cr
	crystal build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-offset: src/bin/offset.cr
	crystal build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-ping: src/bin/ping.cr
	crystal build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-topics: src/bin/topics.cr
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

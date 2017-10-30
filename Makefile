SHELL = /bin/bash
CRYSTAL ?= crystal
LINK_FLAGS = --link-flags "-static"
SRCS = ${wildcard src/bin/*.cr}
PROGS = $(SRCS:src/bin/%.cr=kafka-%)

.PHONY : all build clean test spec test-compile-bin spec-real bin
.PHONY : ${PROGS}

all: build

build: bin ${PROGS}

bin:
	@mkdir -p bin

#kafka-%: src/bin/%.cr
#	${CRYSTAL} build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-httpd: src/bin/httpd.cr
	${CRYSTAL} build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-cluster-watch: src/bin/cluster-watch.cr
	${CRYSTAL} build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-info: src/bin/info.cr
	${CRYSTAL} build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-broker: src/bin/broker.cr
	${CRYSTAL} build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-error: src/bin/error.cr
	${CRYSTAL} build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-fetch: src/bin/fetch.cr
	${CRYSTAL} build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-heartbeat: src/bin/heartbeat.cr
	${CRYSTAL} build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-metadata: src/bin/metadata.cr
	${CRYSTAL} build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-offset: src/bin/offset.cr
	${CRYSTAL} build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-ping: src/bin/ping.cr
	${CRYSTAL} build --release $^ -o bin/$@ ${LINK_FLAGS}

kafka-topics: src/bin/topics.cr
	${CRYSTAL} build --release $^ -o bin/$@ ${LINK_FLAGS}


test: check_version_mismatch test-compile-bin spec

spec:
	${CRYSTAL} spec -v

spec-real:
	${CRYSTAL} spec -v spec-real

test-compile-bin:
	@for x in src/bin/*.cr ; do\
	  ${CRYSTAL} build "$$x" -o /dev/null ;\
	done

clean:
	@rm -rf bin tmp

.PHONY : check_version_mismatch
check_version_mismatch: shard.yml README.md
	diff -w -c <(grep version: README.md) <(grep ^version: shard.yml)


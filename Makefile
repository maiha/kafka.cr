SHELL = /bin/bash
CRYSTAL ?= crystal
LINK_FLAGS = --link-flags "-static"
SRCS = ${wildcard src/bin/*.cr}
PROGS = $(SRCS:src/bin/%.cr=kafka-%)

VERSION=
CURRENT_VERSION=$(shell git tag -l | sort -V | tail -1)
GUESSED_VERSION=$(shell git tag -l | sort -V | tail -1 | awk 'BEGIN { FS="." } { $$3++; } { printf "%d.%d.%d", $$1, $$2, $$3 }')
GIT_REV_ID=`(git describe --tags 2>|/dev/null) || (LC_ALL=C date +"%F-%X")`

.PHONY : all build clean test spec test-compile-bin ci bin
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

ci-setup:
	docker-compose -f ci/docker-compose.yml up -d

ci-teardown:
	docker-compose -f ci/docker-compose.yml stop
	docker-compose -f ci/docker-compose.yml rm -f

ci:
	${CRYSTAL} spec -v ci

test-compile-bin:
	@for x in src/bin/*.cr ; do\
	  ${CRYSTAL} build "$$x" -o /dev/null ;\
	done

clean:
	@rm -rf bin tmp

.PHONY : check_version_mismatch
check_version_mismatch: shard.yml README.md
	diff -w -c <(grep version: README.md) <(grep ^version: shard.yml)

.PHONY : version
version:
	@if [ "$(VERSION)" = "" ]; then \
	  echo "ERROR: specify VERSION as bellow. (current: $(CURRENT_VERSION))";\
	  echo "  make version VERSION=$(GUESSED_VERSION)";\
	else \
	  sed -i -e 's/^version: .*/version: $(VERSION)/' shard.yml ;\
	  sed -i -e 's/^    version: [0-9]\+\.[0-9]\+\.[0-9]\+/    version: $(VERSION)/' README.md ;\
	  echo git commit -a -m "'$(COMMIT_MESSAGE)'" ;\
	  git commit -a -m 'version: $(VERSION)' ;\
	  git tag "v$(VERSION)" ;\
	fi

.PHONY : bump
bump:
	make version VERSION=$(GUESSED_VERSION) -s

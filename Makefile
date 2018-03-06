SHELL = /bin/bash
CRYSTAL ?= crystal
RELEASE=
STATIC ?= ""
LINK_FLAGS = --link-flags ${STATIC}
SRCDIR = ./src/bin
BINDIR = ./bin
SRCS = ${wildcard src/bin/*.cr}
BINS = $(addprefix $(BINDIR)/, $(SRCS:src/bin/%.cr=%))

VERSION=
CURRENT_VERSION=$(shell git tag -l | sort -V | tail -1)
GUESSED_VERSION=$(shell git tag -l | sort -V | tail -1 | awk 'BEGIN { FS="." } { $$3++; } { printf "%d.%d.%d", $$1, $$2, $$3 }')
GIT_REV_ID=`(git describe --tags 2>|/dev/null) || (LC_ALL=C date +"%F-%X")`

.PHONY : all build clean test spec ci bin

all: compile

deps:
	@if [ ! -d lib  ]; then \
	  docker-compose run crystal shards update; \
	fi

compile: deps
	docker-compose run crystal make build

release: deps
	docker-compose run crystal make build RELEASE=--release STATIC=-static

clean:
	rm -rf bin tmp

build: clean $(BINS)

$(BINDIR)/%: $(SRCDIR)/%.cr
	@mkdir -p bin
	${CRYSTAL} build ${RELEASE} $^ -o $@ ${LINK_FLAGS}

umount:
	docker run --rm -t -v $$(pwd)/bin:/mnt alpine chown -R $$(id -u):$$(id -g) /mnt

test: check_version_mismatch compile spec

spec:
	docker-compose run spec

spec-clean:
	docker-compose stop
	docker-compose rm -f

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

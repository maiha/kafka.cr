all: build

build: heartbeat

heartbeat: src/main/heartbeat.cr
	@mkdir -p bin
	crystal build --release src/main/heartbeat.cr -o bin/heartbeat

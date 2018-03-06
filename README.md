# kafka.cr [![Build Status](https://travis-ci.org/maiha/kafka.cr.svg?branch=master)](https://travis-ci.org/maiha/kafka.cr)

`kafka` library and utils for [Crystal](http://crystal-lang.org/).

##### binary (standalone utilities)
- x86_64 binary: https://github.com/maiha/kafka.cr/releases

##### versions
- **kafka**: `1.0`
- **crystal**: `0.24.1`

## Example

```crystal
require "kafka"

kafka = Kafka.new
kafka.topics.map(&.name)  # => ["t1", ...]
kafka.produce "t1", "foo"
kafka.fetch "t1"          # => Kafka::Message("t1#0:0", "foo")
kafka.close
```

## components

- bin: standalone kafka utility applications (x86 static binary)
- lib: as crystal library

## lib

### supported protocols
- https://github.com/maiha/kafka.cr/blob/master/src/kafka/protocol.cr

### Installation

Add it to `shard.yml`

```yml
dependencies:
  kafka:
    github: maiha/kafka.cr
    version: 0.6.5
```

```crystal
require "kafka"

kafka = Kafka.new("localhost", 9092)

kafka.topics.map(&.name)  # => ["t1", ...]
kafka.produce("t1", "test")
kafka.fetch("t1", 0, 0_i64)  # => Kafka::Message("t1[0]#0", "test")

kafka.close
```

## bin

### build

- type `make compile` that generates `bin/kafka-*`
- type `make release` if you wish static and optimized binaries

```shell
% make compile
% make release
```

### created binaries (for utils)

- kafka-broker : Show broker information. "-j" causes json output.
- kafka-cluster-watch : Report cluster information continually.
- kafka-error : Lookup kafka error code.
- kafka-fetch : Fetch logs from kafka. "-g" tries to resolve payload.
- kafka-info : Show topic information about offsets. (need only a broker)
- kafka-ping : Ping to a broker like unix ping.
- kafka-topics : Show topic information about leader, replicas, isrs. (need exact leaders)

### created binaries (for kafka protocols study)

- kafka-heartbeat : Send heartbeat request(api:12). [experimental]
- kafka-metadata : Send metadata request(api:3).
- kafka-offset : Send offset request(api:2).

### kafka-info

```shell
% ./bin/kafka-info t1 t2
t2#0     count=18 [37, 36, 19]
t1#2     count=1 [1, 0]
t1#0     count=1 [1, 0]
t1#1     count=0 [0]
```

- count messages in all topics

```shell
% ./bin/kafka-info -c -a
2       a
0       b
```

### kafka-topics

- `bin/kafka-topics` shows topic names and metadatas

```shell
% ./bin/kafka-topics
t1
tmp

% ./bin/kafka-topics -c | sort -n
0       t1
6       tmp

% ./bin/kafka-topics t1 t2
t1(0 => {leader=1,replica=[1],isr=[1]})
ERROR: t2(UnknownTopicOrPartitionCode (3))
```

### kafka-ping

- `bin/kafka-ping` works like unix `ping` command

```shell
% ./bin/kafka-ping localhost
Kafka PING localhost:9092 (by HeartbeatRequest)
[2016-01-28 00:27:30 +0000] errno=16 from localhost:9092 req_seq=1 time=7.354 ms
[2016-01-28 00:27:31 +0000] errno=16 from localhost:9092 req_seq=2 time=3.433 ms
^C
--- localhost:9092 kafka ping statistics ---
2 requests transmitted, 2 received, ok: 2, error: 0
```

- `-g` option can be used for checking version

```shell
% ./bin/kafka-ping localhost -g
Kafka PING localhost:9092 (by HeartbeatRequest)
[2016-01-28 00:29:16 +0000] (0.8.x) from localhost:9092 req_seq=1 time=8.459 ms
...
```

- write reports about changing state into stderr

```shell
% ./bin/kafka-ping localhost -g
(stdout)
[2016-01-28 00:30:32 +0000] (0.8.x) from localhost:9092 req_seq=76 time=3.194 ms
[2016-01-28 00:30:33 +0000] (0.8.x) from localhost:9092 req_seq=77 time=3.122 ms
[2016-01-28 00:30:34 +0000] (broker is down) from localhost:9092 req_seq= time=0.511 ms
(stderr)
[2016-01-28 00:30:34 +0000] localhost:9092 : (0.8.x) -> (broker is down)
```

## Development

```shell
make compile
```

## Run test : test with real brokers

Run `docker-compose run spec`, or simply `make spec`.

```shell
make spec
```

Docker containers `zk` and `kafka brokers` are automatically created by docker-compose.
- https://github.com/wurstmeister/kafka-docker/blob/master/docker-compose-single-broker.yml

## Contributing

1. Fork it ( https://github.com/maiha/kafka.cr/fork )
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
4. Push to the branch (git push origin my-new-feature)
5. Create a new Pull Request

## Contributors

- [maiha](https://github.com/maiha) maiha - creator, maintainer

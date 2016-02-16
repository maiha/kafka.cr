# kafka.cr

kafka utils for crystal

- still in the alpha stage
- master branch will be force pushed without announcement

## Installation

- `make` generates `bin/kafka-*`
- NOTE: works on only 0.11.1

## Usage

### kafka-topics

- `bin/kafka-topics` shows topic names and metadatas

```
% ./bin/kafka-topics -l
t1

% ./bin/kafka-topics t1
t1

% ./bin/kafka-topics t1 t2
t1
ERROR: t2

% ./bin/kafka-topics t1 t2 -v
t1(0 => {leader=1,replica=[1],isr=[1]})
ERROR: t2(UnknownTopicOrPartitionCode (3))
```

### kafka-ping

- `bin/kafka-ping` works like unix `ping` command

```
% ./bin/kafka-ping localhost
Kafka PING localhost:9092 (by HeartbeatRequest)
[2016-01-28 00:27:30 +0000] errno=16 from localhost:9092 req_seq=1 time=7.354 ms
[2016-01-28 00:27:31 +0000] errno=16 from localhost:9092 req_seq=2 time=3.433 ms
^C
--- localhost:9092 kafka ping statistics ---
2 requests transmitted, 2 received, ok: 2, error: 0
```

- `-g` option can be used for checking version

```
% ./bin/kafka-ping localhost -g
Kafka PING localhost:9092 (by HeartbeatRequest)
[2016-01-28 00:29:16 +0000] (0.8.x) from localhost:9092 req_seq=1 time=8.459 ms
...
```

- write reports about changing state into stderr

```
% ./bin/kafka-ping localhost -g
(stdout)
[2016-01-28 00:30:32 +0000] (0.8.x) from localhost:9092 req_seq=76 time=3.194 ms
[2016-01-28 00:30:33 +0000] (0.8.x) from localhost:9092 req_seq=77 time=3.122 ms
[2016-01-28 00:30:34 +0000] (broker is down) from localhost:9092 req_seq= time=0.511 ms
(stderr)
[2016-01-28 00:30:34 +0000] localhost:9092 : (0.8.x) -> (broker is down)
```

## Development

## Test

- `make test`

## Contributing

1. Fork it ( https://github.com/maiha/kafka.cr/fork )
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
4. Push to the branch (git push origin my-new-feature)
5. Create a new Pull Request

## Contributors

- [[maiha]](https://github.com/maiha) maiha - creator, maintainer

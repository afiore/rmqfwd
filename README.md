# Rmqfwd

Rmqfwd listens to messages published in the `amq.rabbitmq.trace` exchange and persists them in a document store (i.e. Elasticsearch).

<!> _WARNING_: this library is unmaintained <!>

This tool is designed with two main use cases in mind:

- _Message auditing:_ by relying Rabbit's [Firehose tracer feature](https://www.rabbitmq.com/firehose.html), `rmqfwd` allows to inspect all the messages flowing in and out of the cluster, 
irrespectively of the specific app that publishes or consumes them, and its logging behaviour.

- _Eventsourcing and replay:_ In event-based systems, _replaying_ messages constitutes an established pattern to perform a broad range of data operations (e.g. 
backfilling a system, forcing it to recompute a given entity, addressing data inconsistencies introduced by occasional bugs or temporary outages).
By leveraging the search capability of ElasticSearch, `rmqfwd` allows to re-publish to an arbitrary exchange/routing-key
in a single command, without having to write ad-hoc code.


## Usage

```
USAGE:
    rmqfwd [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    trace     Bind a queue to 'amq.rabbitmq.trace' and persists received messages into the message store
    export    Query the message store and write the result to the file system
    replay    Publish a subset of the messages in the data store
    help      Prints this message or the help of the given subcommand(s)
```

### Configuration 

By default, rmqfwd will try to read the `/etc/rmqfwd.toml` file (this might be overridden using the `-c` command line switch).

A default configuration is reproduced below:

```toml
[rabbitmq]
host = "localhost"
port = 5672
tracing_exchange = "amqp.rabbitmq.trace"

[rabbitmq.creds]
user = "guest"
password = "guest"

[elasticsearch]
index = "rabbit_messages"
message_type = "message"
base_url = "http://localhost:9200"
```

### Tracing messages

The `trace` subcommand binds a queue to the [Firehose tracer](https://www.rabbitmq.com/firehose.html) exchange `amq.rabbitmq.trace`.
Dequeued messages are written to ElasticSearch in a canonical format which captures key metadata such as 
(i.e. _published/delivered exchange_, _routing key_, _bound queues_, _headers_, etc), as well as the actual message body as plain text.
The format is deliberately flat and is intended to play well with [Kibana](https://www.elastic.co/products/kibana)'s built-in 
filters. 

## Development setup

Once installed the Rust stable toolchain, you can build from source using:

```
cargo build
```

The compiled executable will then be available at `./target/debug/rmqfwd`

### Running and testing

You can setup a development enviornment by running `docker-compose up` in the project directory. This will initialise the following processes in two separate containers:

- a Rabbitmq instance with [Firehose tracer](https://www.rabbitmq.com/firehose.html) enabled, managment console, and guest user access.
- an Elasticsearch 2.5 instance

A [smoketest](bin/smoketest.sh) is provided and is currently used in CI for regression testing

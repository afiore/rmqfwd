# RMQ-FWD

Utility program to run as a Rabbitmq sidecard.

Rmqfwd listens to messages published in the `amq.rabbitmq.trace` exchange and persists them to Elasticsearch.
Once persisted, messages can be filtered, exported, and re-published.

**Warning:** this project is not feature complete, and is not recommended to use for production purpuses!

## Usage

```
rmqfwd 0.1.0

USAGE:
    rmqfwd [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    export    Query the message store and write the result to the file system
    help      Prints this message or the help of the given subcommand(s)
    replay    Publish a subset of the messages in the data store
    trace     Bind a queue to 'amq.rabbitmq.trace' and persists received messages into the message store
```

## Setup

You can setup a development enviornment by running `docker-compose up` in the project directory. This will setup the following:

- a Rabbitmq instance with [Firehose tracer](https://www.rabbitmq.com/firehose.html) enabled, managment console, and guest user access.
- an Elasticsearch 2.5 instance

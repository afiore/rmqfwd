# RMQ-FWD

Rabbitmq sidecar program to listen to messages in the `amq.rabbitmq.trace` exchange and forward them to Elasticsearch

**Warning:** this project is not feature complete, and is not recommended to use in production!

## Usage

```
rmqfwd 0.1.0                                                                                                                                                                                
USAGE:
    rmqfwd [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    export    fetch a message from the store and write it to the file system
    help      Prints this message or the help of the given subcommand(s)
    replay    replay a set of message ids
    trace     bind a queue to 'amq.rabbitmq.trace' and persists received messages into the message store 
```

## Setup

You can setup a development enviornment by running `docker-compose up` in the project directory. This will setup the following:

- a Rabbitmq instance with [Firehose tracer](https://www.rabbitmq.com/firehose.html) enabled, managment console, and guest user access.
- an Elasticsearch 2.5 instance

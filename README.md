# RMQ-FWD

Rabbitmq sidecar program to listen to messages in the `amq.rabbitmq.trace` exchange and forward them to Elasticsearch

## Setup RabbitMQ:

Initialise a rabbitmq instance

```
docker run -p 5672:5672 -p 15672:15672 -d --hostname my-rabbit --name some-rabbit -e RABBITMQ_ERLANG_COOKIE='secret' rabbitmq:3.6.10-management
```

Then enable the [Firehose tracer](https://www.rabbitmq.com/firehose.html) functionality by executing the following

```
docker run -it --rm --link some-rabbit:my-rabbit -e RABBITMQ_ERLANG_COOKIE='secret' rabbitmq:3 bash
rabbitmqctl --erlang-cookie=secret -n rabbit@my-rabbit trace_on
```

Finally, verify that tracing is enabled by inspecting the main vhost throught the Rabbit Management REST API:

```
curl -u guest:guest http://localhost:15672/api/vhosts | jq '.[0].tracing'
```

You should see the value `true` printed to standard output!


## Setup Elasticsearch:

```
docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" elasticsearch:2.4.6
```

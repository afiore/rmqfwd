rmq_admin=./bin/rabbitmqadmin
exchange=some-exchange
other_exchange=some-other-exchange
queue=some-queue
routing_key=some-key
n_msgs_sent=$((5 + RANDOM % 15))
n_exported=$((2 + RANDOM % 5))


if [ ! -f $rmq_admin ]
then
  curl http://localhost:15672/cli/rabbitmqadmin -o ./bin/rabbitmqadmin
fi

# requires python
./bin/rabbitmqadmin declare exchange name=$exchange type=topic
./bin/rabbitmqadmin declare queue name=$queue
./bin/rabbitmqadmin declare binding source=$exchange destination=$queue routing_key=$routing_key


echo "about to send $n_msgs_sent messages ..."
i=0
while [ $i -lt $n_msgs_sent ]
do
  uuid=$(cat /proc/sys/kernel/random/uuid)
  msgs_sent[$i]="test message $uuid"
  echo "sending message $i..."
  $rmq_admin publish routing_key=$routing_key exchange=$exchange payload="${msgs_sent[$i]}"
  sleep 1
  ((i+=1))
done

# 1. republish first two messages in other exchange/queue, check expected queue count
# 2. export one message, checking target directory contains expected file

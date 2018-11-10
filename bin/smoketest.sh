set -e

rmq_admin=./bin/rabbitmqadmin
exchange=some-exchange
other_exchange=some-other-exchange
queue=some-queue
other_queue=some-other-queue
routing_key=some-key
n_msgs_sent=$((5 + RANDOM % 8))
n_exported=$((2 + RANDOM % 5))
rmqfwd_bin=./target/debug/rmqfwd
rmqadmin_bin=./bin/rabbitmqadmin
export_dir=/tmp/rmqfwd_exports
uuids=()

trap "killall rmqfwd" EXIT 
trap "killall rmqfwd" INT 
$rmqfwd_bin trace &

if [ ! -f $rmq_admin ]
then
  curl http://localhost:15672/cli/rabbitmqadmin -o $rmq_admin
fi

$rmq_admin declare exchange name=$exchange type=topic

$rmq_admin declare exchange name=$other_exchange type=topic

$rmq_admin declare queue name=$queue
$rmq_admin declare queue name=$other_queue

$rmq_admin declare binding source=$exchange destination=$queue routing_key=$routing_key
$rmq_admin declare binding source=$other_exchange destination=$other_queue routing_key=$routing_key

echo "about to publish $n_msgs_sent messages in exchange $exchange ..."
i=0
while [ $i -lt $n_msgs_sent ]
do
  uuid=$(cat /proc/sys/kernel/random/uuid)
  uuids[$i]=$uuid
  echo "sending message $i..."
  $rmq_admin publish routing_key=$routing_key exchange="$exchange" payload="message ${uuids[$i]}"
  sleep 1
  ((i+=1))
done

# 1. republish first two messages in other exchange/queue, check expected queue count
i=0
while [ $i -lt 2 ]
do
  echo "republishing message with uuid: ${uuids[$i]}"
  $rmqfwd_bin replay -b ${uuids[$i]} -e "publish.$exchange" $other_exchange $routing_key
  sleep 1
  ((i+=1))
done

msg_count=$($rmq_admin --format tsv get queue=$other_queue count=10| sed -E 1d | wc -l)
expected=2
if [ "$msg_count" -ne "$expected" ]
then
  echo -e "\e[31mExpecting $expected messages to be republished in $other_queue. Found $msg_count instead!\e[0m"
  exit 1
fi

# 2. export one message, checking target directory contains expected file

$rmqfwd_bin export -f -p -e "publish.$exchange" -b ${uuids[0]} $export_dir
sleep 1
expected=1
file_count=$(find $export_dir -name '*.json' | wc -l)
file_name=$(find $export_dir -name '*.json')

if [ "$file_count" -ne "$expected" ]
then
  echo -e "\e[31Expecting $expected file, found $file_count\e[0m"
  exit 1
else
  exported_uuid=$(jq '.message.body | sub("message "; "")' $file_name | sed -E 's/"//g')
  if [ "$exported_uuid" != "${uuids[0]}" ]
  then
    echo -e "\e[31mExpecting $file_name to contain '${uuids[0]}', found '$exported_uuid' instead.\e[0m"
    exit 1
  fi
fi
echo -e "\e[32mSmoketest passed!\e[0m"

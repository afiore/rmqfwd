set -e -x -u

rmq_username=test_user
rmq_password=test_password
rmq_admin=./bin/rabbitmqadmin
rmq_admin_opts="--username=$rmq_username --password=$rmq_password"
exchange=some-exchange
other_exchange=some-other-exchange
queue=some-queue
other_queue=some-other-queue
routing_key=some-key
n_msgs_sent=$((5 + RANDOM % 8))
n_exported=$((2 + RANDOM % 5))
rmqfwd_bin=./target/debug/rmqfwd
export_dir=/tmp/rmqfwd_exports
es_index=smoketest
es_type=message
rmqfwd_timerange_ops="--since 2010-07-08T09:10:11.012Z --until 2030-07-08T09:10:11.012Z"
es_ops="--es-index $es_index --es-type $es_type"
uuids=()

cat > ./config.toml  <<- EOF
[rabbitmq]
host = "localhost"
port = 5673
tracing_exchange = "amq.rabbitmq.trace"

[rabbitmq.creds]
user = "$rmq_username"
password = "$rmq_password"

[elasticsearch]
index = "$es_index"
message_type = "$es_type"
base_url = "http://localhost:9200"
EOF

trap "killall rmqfwd" EXIT 
trap "killall rmqfwd" INT 
RUST_LOG='rmqfwd=debug,hyper=debug' $rmqfwd_bin trace -c ./config.toml &

if [ ! -f $rmq_admin ]
then
  curl http://localhost:15672/cli/rabbitmqadmin -o $rmq_admin
  chmod +x $rmq_admin
fi

$rmq_admin $rmq_admin_opts declare exchange name=$exchange type=topic

$rmq_admin $rmq_admin_opts declare exchange name=$other_exchange type=topic

$rmq_admin $rmq_admin_opts declare queue name=$queue
$rmq_admin $rmq_admin_opts declare queue name=$other_queue

$rmq_admin $rmq_admin_opts declare binding source=$exchange destination=$queue routing_key=$routing_key
$rmq_admin $rmq_admin_opts declare binding source=$other_exchange destination=$other_queue routing_key=$routing_key

exit_with_error() {
  echo -e "\e[31m$1\e[0m"
  exit 1
}
notice() {
  echo -e "\e[32m$1\e[0m"
}

sleep 5
echo "about to publish $n_msgs_sent messages in exchange $exchange ..."
i=0
while [ $i -lt $n_msgs_sent ]
do
  uuid=$(cat /proc/sys/kernel/random/uuid)
  uuids[$i]=$uuid
  echo "sending message $i..."
  $rmq_admin $rmq_admin_opts publish routing_key=$routing_key exchange="$exchange" payload="message ${uuids[$i]}"
  sleep 1
  ((i+=1))
done

# 1. republish first two messages in other exchange/queue, check expected queue count
i=0
while [ $i -lt 2 ]
do
  echo "republishing message with uuid: ${uuids[$i]}"
  RUST_LOG='rmqfwd=debug' $rmqfwd_bin republish -c ./config.toml -b "${uuids[$i]}" -e $exchange --target-exchange $other_exchange --target-routing-key $routing_key
  sleep 2
  ((i+=1))
done

msg_count=$($rmq_admin $rmq_admin_opts --format tsv get queue=$other_queue count=10| sed -E 1d | wc -l)
expected=2
if [ "$msg_count" -ne "$expected" ]
then
  exit_with_error "Expecting $expected messages to be republished in $other_queue. Found $msg_count instead!"
else
  notice "replay command ok"
fi

# 2. export one single message, checking target directory contains expected file

RUST_LOG=rmqfwd=debug $rmqfwd_bin export -c ./config.toml -f -p -e $exchange -b "${uuids[0]}" $rmqfwd_timerange_ops  $export_dir
sleep 1
expected=1
file_count=$(find $export_dir -name '*.json' | wc -l)
file_name=$(find $export_dir -name '*.json')

if [ "$file_count" -ne "$expected" ]
then
  exit_with_error "Expecting $expected file, found $file_count"
else
  exported_uuid=$(jq '.message.body | sub("message "; "")' $file_name | sed -E 's/"//g')
  if [ "$exported_uuid" != "${uuids[0]}" ]
  then
    exit_with_error "Expecting $file_name to contain '${uuids[0]}', found '$exported_uuid' instead."
  else
    notice "export command ok"
  fi
fi

# 3. Lookup a message using the search endpoint
total_results=$(curl "http://localhost:1337?exchange=$exchange&message-body=${uuids[0]}" | jq '.hits.total')
if [ "$total_results" != "1" ]
then
   exit_with_error "Expecting one single result, found $total_results"
   exit 1
 else
   notice "search endpoint ok"
fi

# 4. export messages by id
ids=($(curl "http://localhost:1337?exchange=$exchange" | jq '.hits.hits[]._id'| head -n 3 | tr -d '"'))
ids_ops=""
for id in "${ids[@]}"
do
  ids_ops="$ids_ops --id $id"
done

RUST_LOG=rmqfwd=debug $rmqfwd_bin export -c ./config.toml $ids_ops -f $export_dir
for id in "${ids[@]}"
do
  id_file="${export_dir}/$id.json"	
  if [ ! -f $id_file ]
  then
    exit_with_error "Expect $id_file to exists."
    exit 1
  else
    notice "$id_file found ..."
  fi
done
notice "Smoketest passed!"

#!/usr/bin/env bash

docker-compose up &

attempts=0
while [ $attempts -lt 10 ]
do
  curl --fail http://localhost:9200/_cluster/health > /dev/null
  if [ $? -eq 0 ]
  then
    echo "Elasticsearch is ready ..."
    break
  else
    echo "couldn't connect to elasticsearch. Retrying in one second"
    sleep 1
    attempts=$[$i+1]
  fi
done

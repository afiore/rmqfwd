#!/usr/bin/env bash

docker-compose up -d

wait_for () {
  attempts=0
  service=$1
  service_uri=$2
  ready=false

  if [ -z "$3" ]
  then
    auth=""
  else
    auth="-u $3"
  fi

  while [ $attempts -lt 30 ]
  do
    curl --silent $auth $service_uri > /dev/null
    if [ $? -eq 0 ]
    then
      echo "$service is ready ..."
      ready=true
      break
    else
      echo "couldn't connect to $service. Retrying in one second"
      sleep 1
      ((attempts+=1))
    fi
  done

  if $ready
  then
    echo "$service is ready!"
    return
  else
    echo "Failed connecting to $service (max attempts reached)"
    exit 1
  fi
}

wait_for "elasticsearch" "http://localhost:9200/_cluster/health"
wait_for "rabbit" "http://localhost:15672/api/vhosts" "guest:guest"

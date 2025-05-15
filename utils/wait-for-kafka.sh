#!/bin/bash
set -e

host="$1"
shift

until nc -z "$host" 9092; do
  echo "Waiting for Kafka at $host:9092..."
  sleep 2
done

exec "$@"

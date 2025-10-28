#!/bin/bash

host="$1"
port="$2"
shift 2
cmd="$@"

until nc -z "$host" "$port"; do
  echo "Waiting for database connection..."
  sleep 1
done

>&2 echo "Database is up - executing command"
exec $cmd
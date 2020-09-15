#!/bin/bash

set -euo pipefail

trap 'kill 0' EXIT

cargo build
PORT=3030
for i in {0..2}
do
  ./target/debug/raft "$i" 3 $PORT > $i.log 2>&1 &
  PORT=$((PORT + 1000))
done

echo "Started workers"

wait

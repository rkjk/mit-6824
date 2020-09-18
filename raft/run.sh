#!/bin/bash

set -euo pipefail

trap 'kill 0' EXIT

cargo build

# Make Sure `config` has enough entries with unique IP:port - In this case 3 entries, each entry
# of the form `IP PORT` followed by newline
for i in {0..2}
do
  ./target/debug/raft "$i" > $i.log 2>&1 &
done

echo "Started workers"

wait

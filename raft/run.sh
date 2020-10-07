#!/bin/bash

set -euo pipefail

trap 'kill 0' EXIT

DEBUG=${1:-nodebug}

cargo build

# Make Sure `config` has enough entries with unique IP:port - In this case 3 entries, each entry
# of the form `IP PORT` followed by newline
if [ $DEBUG = "debug" ]
then
  for i in {0..2}
  do
    RUST_BACKTRACE=1 ./target/debug/raft "$i" > $i.log 2>&1 &
  done
elif [ $DEBUG = "full" ]
then
  for i in {0..2}
  do
    RUST_BACKTRACE=full ./target/debug/raft "$i" > $i.log 2>&1 &
  done
else
  for i in {0..2}
  do
    ./target/debug/raft "$i" > $i.log 2>&1 &
  done
fi

echo "Started workers"

wait

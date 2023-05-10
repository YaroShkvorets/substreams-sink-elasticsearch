#!/usr/bin/env bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

main() {
  cd "$ROOT"

  set -e

  dsn="${ELASTIC_DSN:-"http://localhost:9200"}"
  sink="$ROOT/../../cmd/substreams-sink-elasticsearch/substreams-sink-elasticsearch"
  substreams_spkg="${SUBSTREAMS_SPKG:-"https://github.com/pinax-network/substreams/releases/download/eosio.token-v0.8.1/eosio-token-v0.8.1.spkg"}"

  $sink run \
    ${dsn} \
    dev_db \
    "eos.firehose.eosnation.io:9001" \
    "$substreams_spkg" \
    "db_out" \
    "$@"
}

main "$@"

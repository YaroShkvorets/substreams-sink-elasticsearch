#!/usr/bin/env bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

main() {
  cd "$ROOT"

  set -e

  dsn="${ELASTIC_DSN:-"http://localhost:9200"}"
  sink="$ROOT/../../cmd/substreams-sink-elasticsearch/substreams-sink-elasticsearch"
  #substreams_spkg="${SUBSTREAMS_SPKG:-"https://github.com/YaroShkvorets/substreams-eth-block-meta/releases/download/v0.6.0/substreams-eth-block-meta-v0.6.0.spkg"}"
  #substreams_spkg="${SUBSTREAMS_SPKG:-"https://github.com/streamingfast/substreams-eth-block-meta/releases/download/v0.4.1/substreams-eth-block-meta-v0.4.1.spkg"}"
  substreams_spkg="/Users/shkvo/github/Pinax/substreams-eth-block-meta-my/substreams.spkg"

  $sink run \
    ${dsn} \
    eth-block2 \
    "mainnet.eth.streamingfast.io:443" \
    "$substreams_spkg" \
    "db_out" \
    "$@"
}

main "$@"

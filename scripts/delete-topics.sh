#!/bin/bash

set -e -u 

kafka-topics --bootstrap-server localhost:9092 --list | \
    grep "spring-json-ks-app\|spring-avro-ks-app\|pvrbi-\|pvgpk-\|pages-\|pageviews-\|pvbp-\|epv-" | \
    while read line; do 
  set -x
  kafka-topics --bootstrap-server localhost:9092 --delete --topic $line; 
  set +x
done

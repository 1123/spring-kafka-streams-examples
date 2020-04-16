#!/bin/bash

set -e -u 

kafka-topics --bootstrap-server localhost:9092 --list | \
    grep "_confluent-controlcenter" | \
    while read line; do 
  set -x
  kafka-topics --bootstrap-server localhost:9092 --delete --topic $line; 
  set +x
done

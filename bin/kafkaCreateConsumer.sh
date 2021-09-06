#!/usr/bin/env bash
docker rm --tag tap:kafka
docker build ../kafka/ --tag tap:kafka
docker run -e KAFKA_ACTION=consumer -e KAFKA_TOPIC=$1 --network tap   -it tap:kafka

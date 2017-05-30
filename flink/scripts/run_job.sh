#!/bin/bash

flink run -m yarn-cluster -yn 2 s3://emr-cluster-spark-bucket/flink-0.1.jar --input ec2-54-172-148-19.compute-1.amazonaws.com:9092 input-topic output-topic

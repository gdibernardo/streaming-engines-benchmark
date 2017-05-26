#!/bin/bash

echo "Installing Java 8....."
sudo yum install java-1.8.0
sudo yum remove java-1.7.0-openjdk

echo "wgetting Kafka 0.10.... Scala version: 2.11"

wget http://mirror.fibergrid.in/apache/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz

echo "Extracting Kafka.tgz ....."

tar -xzf kafka_2.11-0.10.2.0.tgz
rm kafka_2.11-0.10.2.0.tgz

echo "Configuring .bashrc ...."

echo "export KAFKA_HEAP_OPTS=\"-Xmx6G -Xms6G\"" >> ~/.bashrc
source ~/.bashrc

echo "Starting Zookeeper server.... check log file at ~/zookeeper-logs"
nohup kafka_2.11-0.10.2.0/bin/zookeeper-server-start.sh kafka_2.11-0.10.2.0/config/zookeeper.properties > ~/zookeeper-logs & 
echo "Starting Kafka server.... check log file at ~/kafka-logs"
nohup kafka_2.11-0.10.2.0/bin/kafka-server-start.sh kafka_2.11-0.10.2.0/config/server.properties > ~/kafka-logs &

echo "Done."

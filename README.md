# Kafka Loadtest
##Requirements
1.Docker
2.Python 2.7 (3.x one day?)

## Setup
#### Run kafka and zookeeper in a docker container (https://github.com/spotify/docker-kafka)
```bash
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=`docker-machine ip \`docker-machine active\`` --env ADVERTISED_PORT=9092 spotify/kafka
```
## Exec into the container and create your topics with the appropriate parameters
creating a topic with 12 partitions, rf of 2, and 1 hour retention:
```bash
docker exec -ti <docker id> bash
/usr/src/kafka_2.12-0.11.0.2/bin/kafka-topics.sh --create --topic base-p12-rf2 --replication-factor 2 --partitions 12 --config retention.ms=3600000
```
## Tool help
```bash
python kafka-loadtest.py -h
```
## Running the tool on your laptop against your local cluster
```bash
python kafka-loadtest.py --influxUrl http://localhost:8086 --loadTests laptop-load-test.conf \
--kafkaPerfTest /usr/local/bin/kafka-producer-perf-test --fast
```

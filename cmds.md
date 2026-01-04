# Flink Setup and Execution Walkthrough

## Prerequisites
Docker and Docker Compose installed.

## Setup
To start the cluster (JobManager + TaskManager): 

```bash
docker-compose up -d
```

## Running Examples

### 1. WordCount
To run the standard WordCount example on the new cluster:

```bash
docker-compose exec jobmanager ./bin/flink run examples/streaming/WordCount.jar
```

Reads from a file in the example resources and counts the words.

#### Check Logs:

```bash
docker-compose logs -f taskmanager
```

### 2. SocketWindowWordCount
To run the socket example, you can still use an ad-hoc stream source container connected to the compose network.

#### Start Stream:

```bash
docker run -d --rm --name stream-source --network punctuation-semantic_default alpine sh -c "while true; do echo 'hello world flink'; sleep 2; done | nc -lk -p 9999"
```
Create a socket server that emits the string "hello world flink" every second.
(Note: Network name punctuation-semantic_default is the default created by compose. Check docker network ls if unsure).

#### Submit Job:

```bash
docker-compose exec -d jobmanager ./bin/flink run examples/streaming/SocketWindowWordCount.jar --hostname stream-source --port 9999
```

#### Check Logs:

```bash
docker-compose logs -f taskmanager
```

#### Cleanup
To stop and remove the Flink cluster:

```bash
docker-compose down
```
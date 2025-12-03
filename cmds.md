# Getting started

```bash
docker pull flink:scala_2.12-java21
docker network create flink-network
docker run -d --name=jobmanager --network flink-network -p 8081:8081 flink:scala_2.12-java21 jobmanager
```
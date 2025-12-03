# Getting started

```bash
docker pull flink:scala_2.12-java21
docker network create flink-network
docker run -d --name=jobmanager --network flink-network -p 8081:8081 flink:scala_2.12-java21 jobmanager
docker run -d --name=taskmanager --network flink-network -e JOB_MANAGER_RPC_ADDRESS=jobmanager flink:scala_2.12-java21 taskmanager

docker cp SocketWindowWordCount.jar jobmanager:/opt/flink
docker exec -it jobmanager /opt/flink/bin/flink run /opt/flink/SocketWindowWordCount.jar
```
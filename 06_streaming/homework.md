# Ingest streaming data into Redpanda and consume by Spark

Follow the steps described in week6 DTC github. 

## Installation

Install redpanda

```json
version: '3.7'
services:
  # Redpanda cluster
  redpanda-1:
    image: docker.redpanda.com/vectorized/redpanda:v22.3.5
    container_name: redpanda-1
    command:
      - redpanda
      - start
      - --smp
      - '1'
      - --reserve-memory
      - 0M
      - --overprovisioned
      - --node-id
      - '1'
      - --kafka-addr
      - PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092
      - --advertise-kafka-addr
      - PLAINTEXT://redpanda-1:29092,OUTSIDE://localhost:9092
      - --pandaproxy-addr
      - PLAINTEXT://0.0.0.0:28082,OUTSIDE://0.0.0.0:8082
      - --advertise-pandaproxy-addr
      - PLAINTEXT://redpanda-1:28082,OUTSIDE://localhost:8082
      - --rpc-addr
      - 0.0.0.0:33145
      - --advertise-rpc-addr
      - redpanda-1:33145
    ports:
      # - 8081:8081
      - 8082:8082
      - 9092:9092
      - 28082:28082
      - 29092:29092
```

```bash
docker compose up -d
```

```bash
[spark]$ ls 
spark-worker.Dockerfile
spark-master.Dockerfile
spark-base.Dockerfile
jupyterlab.Dockerfile
docker-compose.yml
cluster-base.Dockerfile
build.sh

[spark]$ chmod u+x build.sh
[spark]$ ./build.sh
```
### Python environment

```bash
python3.8 -m venv .venv
```
```python
pip3.8 install -r requirements.txt
```
```bash
cat requirements.txt
kafka-python==1.4.6
confluent_kafka
requests
avro
faust
fastavro
```

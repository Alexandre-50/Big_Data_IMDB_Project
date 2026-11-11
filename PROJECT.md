# IMDb Big Data Processing Project

## Contributors

| Name | Email |
|------|-------|
| Alexandre Celles |alexandre.celles@edu.ece.fr |
| Romain Arcis |romain.arcis@edu.ece.fr |

## Prerequisites

The whole project runs inside differents Docker containers which are Apache Spark and Apache Kafka and do not need to be installed on the host machine.

### Software

- Git (https://git-scm.com/downloads) (Git Bash on Windows) to clone the repository
- Docker Desktop (https://docs.docker.com/get-started/get-docker/) with Docker Compose, to run the Spark and Kafka containers
- A web browser to access JupyterLab as Firefox, Chrome or anything else

### Hardware

- At least **8 GB of RAM** because Spark and Kafka are memory-intensive
- At least **20 GB of free disk space** with Docker images (~8 GB), IMDb dataset and its Parquet conversion

### Versions used

All versions are pinned in `docker-compose.yml`, so the environment is identical on every machine.

| Component | Version |
|-----------|---------|
| Apache Spark (PySpark) | 4.2.0 |
| Scala | 2.13 |
| Java (inside the container) | 21 |
| Apache Kafka (KRaft mode, no Zookeeper) | 3.8.0 |
| Spark image | `quay.io/jupyter/pyspark-notebook:spark-4.2.0` |

## Environment Setup

### 1. Clone the repository

```bash
git clone https://github.com/Alexandre-50/Big_Data_IMDB_Project.git
cd Big_Data_IMDB_Project
```

### 2. Start the containers

Make sure Docker Desktop is running, then start the environment:

```bash
docker compose up -d
```

The first run downloads the images (about 8 GB), which can take several minutes.

Check that both containers are running:

```bash
docker compose ps
```

Two containers must appear with the status `Up`:

| Container | Role | Ports |
|-----------|------|-------|
| `imdb_spark` | JupyterLab and PySpark | 8888 (Jupyter), 4040-4041 (Spark UI) |
| `imdb_kafka` | Kafka broker | 9092 |

![Containers running](images/containers_running.png)

### 3. Access JupyterLab

Get the access link (with its token) from the container logs:

```bash
docker logs imdb_spark
```

Open the most recent link starting with `http://127.0.0.1:8888/lab?token=...`
in a browser. The repository is mounted in the `work/` folder, so every file
created in Jupyter is directly saved in the repository.

### 4. Check that Spark works

In a new notebook inside `work/`, run:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("test-env")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

print(spark.version)
spark.range(5).show()
```

The output must display `4.2.0` followed by a table with the numbers 0 to 4.

![Spark test in Jupyter](images/jupyter_spark_test.png)

### 5. Stop the environment

```bash
docker compose down
```

## Running the Project

### Data Preparation

To be completed.

### Data Exploration

To be completed.

### Data Enrichment

To be completed.

### Streaming

The streaming part needs the Parquet tables (see Data Preparation) and both
containers running (`docker start imdb_kafka` if Kafka was stopped). On
Windows Git Bash, run `export MSYS_NO_PATHCONV=1` first.

1. Install the Python dependencies of the producer (once per container):

   ```bash
   docker exec -w /home/jovyan/work imdb_spark pip install -r requirements.txt
   ```

2. Start the producer, which forwards the Wikimedia `recentchange` stream
   to the Kafka topic `wikipedia-changes` (here for 35 minutes, in the
   background):

   ```bash
   docker exec -d -w /home/jovyan/work -e PYTHONPATH=/home/jovyan/work imdb_spark \
     sh -c "python -m src.streaming.wikipedia_producer --duration 2100 > /tmp/producer.log 2>&1"
   ```

3. Run the Spark Structured Streaming pipeline for 30 minutes. The first
   run also builds the IMDb lookup table in `data/processed/streaming_lookup`:

   ```bash
   docker exec -w /home/jovyan/work -e PYTHONPATH=/home/jovyan/work imdb_spark \
     spark-submit --driver-memory 2g \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
     src/streaming/streaming_pipeline.py --duration 1800
   ```

   Matched events are printed in the console every 30 seconds and saved in
   `results/streaming/` (Parquet), with checkpoints in `checkpoints/streaming/`.
   Delete both folders to start a new test from scratch.

4. Compute the metrics of the run (written to `results/streaming/summary.md`):

   ```bash
   docker exec -w /home/jovyan/work -e PYTHONPATH=/home/jovyan/work imdb_spark \
     spark-submit --driver-memory 2g src/streaming/streaming_report.py
   ```

### Open Question

To be completed.

## AI/LLM Tools Declaration

| Tool | Tasks |
|------|-------|
| Claude (Anthropic) | ... |
| Claude (Anthropic) | Streaming code (Kafka producer, Spark Structured Streaming pipeline, metrics script) and debugging, reviewed and tested by us. Not used for the explanations of REPORT.md. |
| 
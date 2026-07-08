# Version Matrix

Decision date: 2026-07-06

## Development environment

| Component | Selected version | Reason |
|---|---:|---|
| Python | 3.11.15 | Stable Python 3.11 security release; supported by Airflow |
| Java | 17 | Common supported Java version for Kafka and Spark 3.5 |
| Apache Kafka | 4.3.1 | Current Kafka bug-fix release; official Docker image |
| Apache Spark | 3.5.8 | Stable maintenance release in Spark 3.5 branch |
| Delta Lake | 3.3.1 | Delta 3.3.x is compatible with Spark 3.5.x |
| Scala binary | 2.12 | Selected for Spark/Delta package compatibility |
| Apache Airflow | 3.2.2 | Supports Python 3.11 and PostgreSQL 17 |
| PostgreSQL | 17 | Supported by Airflow 3.2.2 |

## Docker images

| Service | Image reference | Digest |
|---|---|---|
| Python | python:3.11.15-slim-bookworm | Fill after pull |
| Kafka | apache/kafka:4.3.1 | Fill after pull |
| PostgreSQL | To be finalized on Day 2 | |
| MinIO | To be finalized on Day 2 | |
| Spark | To be finalized on Day 3 | |
| Airflow | To be finalized on Day 3 | |

## Compatibility decisions

- Java 17 is used as the JVM baseline.
- Spark 3.5.8 is paired with Delta Lake 3.3.1.
- Kafka uses KRaft mode.
- ZooKeeper is not used.
- Docker image tag `latest` is forbidden.
- Image digests will be recorded after images are pulled.

## Upgrade policy

A version change must include:

1. Reason for upgrade.
2. Compatibility review.
3. Test results.
4. Updated image digest.
5. Updated documentation.
6. A separate Git commit.
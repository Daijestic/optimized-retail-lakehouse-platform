# Week 2 Notes

## Day 1 — Kafka Consumer Group and Offsets

### Topic

- Topic: `retail-payment-events`
- Partitions: `3`
- Replication factor: `1`

### End offsets before consuming

| Partition | Earliest offset | End offset |
|---:|---:|---:|
| 0 |  |  |
| 1 |  |  |
| 2 |  |  |

### Consumer group experiment

- Group ID: `bronze-learning-v1`

| Partition | Committed offset | End offset | Lag |
|---:|---:|---:|---:|
| 0 |  |  |  |
| 1 |  |  |  |
| 2 |  |  |  |

### Restart result

- Same group resumed from:
- Did it read old committed records again?
- Lag after restart:

### Different group result

- New group:
- Starting position:
- Did it read the retained history?

### Parallel consumers

- Consumer 1 assignments:
- Consumer 2 assignments:
- Rebalance observed:

### Decisions for Bronze

- `group.id`: `bronze-ingestion-v1`
- `enable.auto.commit`: `false`
- `auto.offset.reset`: `earliest`
- Delivery semantics: at-least-once
- Commit only after successful Bronze write.

---

## Day 2 — Python Raw Kafka Consumer

### Implementation

- Client: `confluent-kafka 2.15.0`
- Topic: `retail-payment-events`
- Group ID: `bronze-ingestion-v1`
- Bootstrap server: `localhost:9092`
- `auto.offset.reset`: `earliest`
- `enable.auto.commit`: `false`
- `enable.auto.offset.store`: `false`

### Raw fields captured

- Key
- Value
- Topic
- Partition
- Offset
- Kafka timestamp
- Headers

### Verification

- Consumer connected successfully:
- Assigned partitions:
- Number of records read:
- Valid record observed:
- Duplicate record observed:
- Late record observed:
- Malformed record observed:
- Negative amount record observed:
- Unsupported schema version observed:

### Offset experiment

- First run offsets:
- Second run offsets:
- Previously seen offsets were read again: yes/no
- Explanation: no offsets were committed.

### Decisions

- Keep key and value as bytes.
- Do not parse JSON in the raw consumer.
- Do not commit until MinIO Bronze write succeeds.
- Bronze delivery guarantee will be at-least-once.

---

## Day 3 — Kafka to MinIO Bronze Raw

### Storage configuration

- Endpoint: `http://localhost:9000`
- Bucket: `lakehouse`
- Prefix: `bronze/events/_unpartitioned`
- Object format: JSON Lines
- Raw byte encoding: Base64
- Content type: `application/x-ndjson`

### Batch policy

- Batch size:
- Batch wait seconds:
- Object count:
- Record count:

### Object key policy

```text
bronze/events/_unpartitioned/
topic=<topic>/
partition=<partition>/
offsets=<start>-<end>.jsonl
```

Object keys are deterministic from Kafka source ranges.

### Durability policy

1. Poll Kafka records.
2. Serialize raw bytes.
3. Upload partition objects.
4. Verify `ContentLength` and SHA-256.
5. Commit `max_offset + 1` per partition.

### Evidence

| Partition | Start offset | End offset | Record count | Object key |
|---:|---:|---:|---:|---|
| 0 |  |  |  |  |
| 1 |  |  |  |  |
| 2 |  |  |  |  |

### Consumer group evidence

| Partition | Current offset | Log-end offset | Lag |
|---:|---:|---:|---:|
| 0 |  |  |  |
| 1 |  |  |  |
| 2 |  |  |  |

### Failure test

- MinIO stopped:
- Bronze write failed:
- Offset advanced: no
- MinIO restarted:
- Same records consumed again:
- Write succeeded:
- Offset committed:

### Current limitations

- No `ingestion_run_id` yet.
- No `ingestion_time` yet.
- No `processing_date` partition yet.
- No event field parsing.
- No replay command.
- No full rebalance flush handling.
---

## Day 4 — Bronze Metadata and Audit Context

### Metadata categories

- Source metadata:
  - source_topic
  - source_partition
  - source_offset
  - source_record_id
  - Kafka timestamp
  - headers

- Ingestion metadata:
  - ingestion_run_id
  - ingestion_batch_number
  - ingestion_batch_id
  - ingestion_time

- Best-effort event metadata:
  - event_id
  - event_type
  - event_time
  - producer_time
  - schema_version
  - payload_parse_status

### Time policy

All ingestion timestamps are timezone-aware UTC values serialized as
ISO-8601 text ending in `Z`.

### Parse policy

Bronze parsing is best-effort only.

- malformed JSON is preserved;
- invalid UTF-8 is preserved;
- negative amount is not rejected;
- unsupported schema version is not rejected;
- raw Kafka bytes remain authoritative.

### Live evidence

- Ingestion run ID:
- Record count:
- Object count:
- Parsed-object count:
- Invalid-JSON count:
- Consumer lag after run:

### Current limitations

- Historical Day 3 objects do not contain the additive Day 4 fields.
- Object path is still under `_unpartitioned`.
- `processing_date` partitioning is implemented on Day 5.
- PostgreSQL ingestion-run metadata is deferred to hardening scope.

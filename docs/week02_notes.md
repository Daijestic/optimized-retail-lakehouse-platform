# Week 2 Notes

## Day 1 — Kafka Consumer Group and Offsets

### Topic

- Topic: retail-payment-events
- Partitions: 3
- Replication factor: 1

### End offsets before consuming

| Partition | Earliest offset | End offset |
|---|---:|---:|
| 0 |  |  |
| 1 |  |  |
| 2 |  |  |

### Consumer group experiment

- Group ID: bronze-learning-v1

| Partition | Committed offset | End offset | Lag |
|---|---:|---:|---:|
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

- group.id: bronze-ingestion-v1
- enable.auto.commit: false
- auto.offset.reset: earliest
- delivery semantics: at-least-once
- commit only after successful Bronze write
## Day 2 — Python raw Kafka consumer

### Implementation

- Client: confluent-kafka 2.15.0
- Topic: retail-payment-events
- Group ID: bronze-ingestion-v1
- Bootstrap server: localhost:9092
- auto.offset.reset: earliest
- enable.auto.commit: false
- enable.auto.offset.store: false

### Raw fields captured

- key
- value
- topic
- partition
- offset
- Kafka timestamp
- headers

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
- Explanation: no offsets were committed

### Decisions

- Keep key and value as bytes.
- Do not parse JSON in the raw consumer.
- Do not commit until MinIO Bronze write succeeds.
- Bronze delivery guarantee will be at-least-once.
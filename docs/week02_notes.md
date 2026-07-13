# Week 2 Notes — Kafka to Immutable Bronze

## Week objective

Build the ingestion path:

```text
Synthetic producer
→ Apache Kafka
→ Python consumer group
→ immutable Bronze JSONL objects in MinIO
```

Bronze preserves raw Kafka records and technical source metadata for audit, replay, debugging, and later Silver processing. It does not validate business rules or remove duplicate and malformed records.

---

## Day 1 — Kafka Consumer Groups and Offsets

### Topic configuration

| Setting | Value |
|---|---|
| Topic | `retail-payment-events` |
| Partitions | `3` |
| Replication factor | `1` |
| Local broker mode | Single-broker Kafka KRaft |

### Concepts verified

- An offset is scoped to one Kafka partition.
- Consumers in the same group share partitions.
- Different groups maintain independent committed offsets.
- A committed offset is the position of the next record to read.
- Consumer lag is calculated from the log-end offset and committed offset.
- `auto.offset.reset=earliest` applies only when a valid committed offset does not exist.

### Consumer-group experiment

- Learning group: `bronze-learning-v1`
- The group was stopped and restarted to verify resume behavior.
- A second group was used to verify that retained Kafka history could be read independently.
- Multiple consumers in one group were used to observe partition assignment and rebalance behavior.

### Final observed state

| Partition | Committed offset | Log-end offset | Lag |
|---:|---:|---:|---:|
| 0 | 194 | 194 | 0 |
| 1 | 150 | 150 | 0 |
| 2 | 160 | 160 | 0 |

### Decisions for Bronze

| Decision | Value |
|---|---|
| Consumer group | Stable group ID |
| `enable.auto.commit` | `false` |
| `enable.auto.offset.store` | `false` |
| `auto.offset.reset` | `earliest` |
| Delivery semantics | At-least-once |
| Commit point | Only after a successful durable Bronze write |

---

## Day 2 — Python Raw Kafka Consumer

### Implementation

| Setting | Value |
|---|---|
| Client | `confluent-kafka==2.15.0` |
| Topic | `retail-payment-events` |
| Bootstrap server from Windows host | `localhost:9092` |
| Main Bronze group | `bronze-ingestion-day03-v1` |
| `auto.offset.reset` | `earliest` |
| `enable.auto.commit` | `false` |
| `enable.auto.offset.store` | `false` |

### Raw Kafka fields captured

- `key`
- `value`
- `topic`
- `partition`
- `offset`
- Kafka timestamp type and timestamp
- headers

### Consumer behavior

- Kafka key and value remain `bytes | None`.
- The raw consumer does not call `json.loads()` on the message value.
- Malformed JSON therefore does not crash or disappear from ingestion.
- Partition assignment, revocation, and loss callbacks are logged.
- `consumer.close()` is called during shutdown.
- No offset is committed until the Bronze writer confirms durable storage.

### Verification

- The consumer connected to `localhost:9092`.
- It subscribed to all three topic partitions.
- It read valid, duplicate, late, malformed, negative-amount, and unsupported-schema-version records without applying business validation.
- Restarting a no-commit test group caused uncommitted records to be read again, as expected.

---

## Day 3 — Kafka to MinIO Bronze Raw

### Storage configuration

| Setting | Value |
|---|---|
| Endpoint | `http://localhost:9000` |
| Bucket | `lakehouse` |
| Development prefix | `bronze/events/_unpartitioned` |
| Object format | JSON Lines |
| Content type | `application/x-ndjson` |
| Raw byte encoding | Base64 |

### Object-key policy

```text
bronze/events/_unpartitioned/
topic=<topic>/
partition=<partition>/
offsets=<start-offset>-<end-offset>.jsonl
```

One object contains records from only one Kafka partition. Within a processing date and a source range, the key is deterministic from:

```text
topic + partition + start_offset + end_offset
```

### Durability and commit sequence

1. Poll Kafka records.
2. Group the batch by Kafka partition.
3. Serialize raw key/value bytes as Base64 inside JSONL envelopes.
4. Upload one object for each represented Kafka partition.
5. Verify object `ContentLength` and the stored checksum metadata.
6. Commit `max_source_offset + 1` for each successfully persisted partition batch.

The custom SHA-256 value stored in object metadata verifies that the expected checksum metadata was persisted. A full stored-body checksum verification requires downloading the object body or using a supported S3 checksum response.

### Reconciliation evidence

| Partition | Bronze offset coverage | Records | Objects | Kafka current offset | Kafka log-end offset | Lag |
|---:|---|---:|---:|---:|---:|---:|
| 0 | `0–383` | 384 | 5 | 384 | 384 | 0 |
| 1 | `0–299` | 300 | 4 | 300 | 300 | 0 |
| 2 | `0–319` | 320 | 4 | 320 | 320 | 0 |
| **Total** | — | **1004** | **13** | — | — | **0** |

### Assertions

- Kafka source records: `1004`
- Bronze JSONL rows: `1004`
- Missing source offsets: `0`
- Overlapping source ranges: `0`
- Consumer lag after ingestion: `0`
- Result: **PASS**

### Failure test

The intended invariant is:

```text
Bronze write or verification failure
→ Kafka offsets are not committed
→ the same source records remain available after restart
```

Live evidence for a failure injected after polling but before object persistence should be recorded separately. Stopping MinIO before the writer starts only verifies startup failure, not the complete post-poll failure path.

### Limitations at the end of Day 3

- No ingestion-run metadata in historical Day 3 rows.
- No processing-date storage partition.
- No replay command.
- No Silver validation or deduplication.
- Full rebalance-aware batch flushing is deferred to hardening.

---

## Day 4 — Bronze Metadata and Audit Context

### Metadata categories

#### Source metadata

- `source_record_id`
- `source_topic`
- `source_partition`
- `source_offset`
- `kafka_timestamp_type`
- `kafka_timestamp_ms`
- headers

#### Ingestion metadata

- `ingestion_run_id`
- `ingestion_batch_number`
- `ingestion_batch_id`
- `ingestion_time`

#### Best-effort event metadata

- `payload_parse_status`
- `event_id`
- `event_type`
- `event_time`
- `producer_time`
- `schema_version`

### Time policy

- Ingestion timestamps are timezone-aware UTC values.
- They are serialized as ISO 8601 strings ending in `Z`.
- `ingestion_time` is created by the Bronze pipeline.
- `event_time`, `producer_time`, and Kafka timestamp remain separate concepts.

### Parse policy

Bronze parsing is best effort only:

- malformed JSON is preserved;
- invalid UTF-8 is preserved;
- a negative amount is not rejected;
- an unsupported schema version is not rejected;
- raw Kafka key/value bytes remain authoritative;
- business validation is deferred to Silver.

### Identity policy

```text
source_record_id = <topic>:<partition>:<offset>
```

`source_record_id` identifies a Kafka record. It is intentionally different from `event_id`, because controlled duplicate events can share one `event_id` while occupying different Kafka offsets.

### Live evidence to record

| Metric | Result |
|---|---|
| Ingestion run ID | |
| Record count | |
| Object count | |
| `parsed_object` count | |
| `invalid_json` count | |
| Kafka lag after run | |

### Compatibility note

Objects written before Day 4 use the earlier additive Bronze envelope and do not contain all Day 4 ingestion metadata. A clean end-to-end rerun at the end of Week 2 should be used for final demonstration evidence.

---

## Day 5 — Processing-Date Partitioning

### Date policy

| Rule | Value |
|---|---|
| Timezone | UTC |
| Source field | `ingestion_time` |
| Format | `YYYY-MM-DD` |
| Business event time used for partitioning | No |

`processing_date` represents when the Bronze pipeline ingested the data. It is not derived from `event_time`, `producer_time`, the Windows local date, or the Kafka record timestamp.

### Object layout

```text
bronze/events/
processing_date=<YYYY-MM-DD>/
topic=<kafka-topic>/
partition=<kafka-partition>/
offsets=<start-offset>-<end-offset>.jsonl
```

The `key=value` segments are storage prefixes that support partition discovery by file-based processing engines. They are not physical filesystem directories.

### Consistency invariant

For every partitioned Bronze object:

```text
processing_date in object key
=
processing-date in object metadata
=
processing_date in every JSONL row
```

### Design decision

`ingestion_run_id` remains in row-level metadata, object-level metadata, and structured logs. It is not used as a physical storage partition because it has high cardinality and would create many small run-specific prefixes.

### Historical objects

Objects produced before Day 5 remain under:

```text
bronze/events/_unpartitioned/
```

They are development artifacts and should be excluded from processing-date replay. They can be removed only after the partitioned path has been verified and a clean final Week 2 run has been captured.

### Idempotency limitation

The object key is deterministic for the same:

```text
processing_date + topic + partition + offset range
```

If the same uncommitted Kafka range is retried on a different UTC date, at-least-once delivery can create a second object under a different `processing_date`. Downstream processing must use source coordinates for reconciliation and deduplication.

### Live evidence to record

| Metric | Result |
|---|---|
| Processing date | |
| Ingestion run ID | |
| Object count | |
| Record count | |
| Path/metadata/row consistency | |
| Kafka lag after ingestion | |

---

## Remaining Week 2 work

- Implement replay by `processing_date`.
- Verify replay does not mutate Bronze objects.
- Document the replay strategy.
- Run a clean end-to-end Week 2 verification.
- Capture final commands, logs, object counts, source-offset reconciliation, and limitations.

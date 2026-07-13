# Bronze Layer

## Purpose

The Bronze layer preserves Kafka records in their raw, replayable form for:

- audit;
- source-to-storage reconciliation;
- debugging upstream producers;
- replay and downstream reprocessing;
- preparation for Silver validation, deduplication, late-event handling, and DLQ routing.

Bronze is a technical ingestion layer. It is not a business-clean data layer.

---

## Data flow

```text
Synthetic producer
→ Kafka topic: retail-payment-events
→ Python raw consumer
→ Bronze JSONL objects in MinIO
→ synchronous Kafka offset commit
```

The consumer uses at-least-once delivery semantics:

```text
persist and verify Bronze objects
→ commit next Kafka offsets
```

If persistence fails before commit, the records remain available to be consumed again.

---

## Core guarantees

- Kafka key and value bytes are preserved using Base64.
- Source topic, partition, and offset are retained for every record.
- Malformed, duplicate, late, negative-amount, and unsupported-schema-version records are not dropped.
- Business validation is not required for Bronze persistence.
- One JSONL object contains records from only one Kafka partition.
- Kafka offsets are committed only after all objects for the flushed batch have been persisted and verified.
- Every ingestion execution has one `ingestion_run_id`.
- Every flushed batch has one run-scoped `ingestion_batch_id`.
- Ingestion timestamps are timezone-aware UTC values.
- `processing_date` is derived from UTC `ingestion_time`.

---

## Delivery semantics

The project currently provides at-least-once ingestion, not end-to-end exactly-once delivery.

### Normal path

```text
poll Kafka
→ create Bronze envelopes
→ write MinIO objects
→ verify size and checksum metadata
→ commit max_offset + 1 per partition
```

### Failure before commit

```text
object write or verification fails
→ no Kafka offset commit
→ records are read again after restart
```

A repeated source range uses the same object key only when the `processing_date`, topic, partition, and offset range are unchanged.

---

## Bronze record schema

| Field | Type | Required | Meaning |
|---|---|---:|---|
| `record_version` | string | yes | Version of the Bronze envelope |
| `ingestion_run_id` | string | yes | Identity of one ingestion execution |
| `ingestion_batch_number` | integer | yes | One-based batch number within the run |
| `ingestion_batch_id` | string | yes | Run-scoped batch identity |
| `ingestion_time` | UTC timestamp | yes | Time the Bronze batch context was created |
| `processing_date` | date string | yes | UTC date derived from `ingestion_time` |
| `source_record_id` | string | yes | Technical identity: `topic:partition:offset` |
| `source_topic` | string | yes | Kafka topic |
| `source_partition` | integer | yes | Kafka partition |
| `source_offset` | integer | yes | Kafka source offset |
| `kafka_timestamp_type` | integer | yes | Kafka timestamp type reported by the client |
| `kafka_timestamp_ms` | integer/null | no | Kafka record timestamp in milliseconds |
| `key_base64` | string/null | no | Original Kafka key bytes encoded as Base64 |
| `value_base64` | string/null | no | Original Kafka value bytes encoded as Base64 |
| `headers` | array | yes | Kafka headers with Base64-encoded values |
| `payload_parse_status` | string | yes | Result of best-effort JSON parsing |
| `event_id` | string/null | no | Best-effort extracted event ID |
| `event_type` | string/null | no | Best-effort extracted event type |
| `event_time` | string/null | no | Best-effort extracted event timestamp |
| `producer_time` | string/null | no | Best-effort extracted producer timestamp |
| `schema_version` | string/null | no | Best-effort extracted payload schema version |

The authoritative payload is `value_base64`. Extracted event fields are convenience metadata and are not validated business data.

---

## Parse statuses

| Status | Meaning |
|---|---|
| `parsed_object` | Payload is valid UTF-8 JSON and the top-level value is an object |
| `invalid_json` | Payload is UTF-8 text but not valid JSON |
| `invalid_utf8` | Payload cannot be decoded as UTF-8 |
| `json_not_object` | Payload is valid JSON but the top-level value is not an object |
| `null_payload` | Kafka message value is null |

A negative amount or unsupported schema version can still have `payload_parse_status=parsed_object`, because these are business or contract violations rather than JSON parsing failures.

---

## Source identity

The technical source identity is:

```text
source_record_id = <topic>:<partition>:<offset>
```

This identity is unique for a Kafka record within the retained topic history.

It is intentionally different from `event_id`:

```text
same event_id
+ different Kafka offsets
= controlled or transport duplicate records
```

Bronze keeps both records. Silver later applies event-level deduplication.

---

## Processing-date partitioning

Bronze objects use UTC ingestion-date partitioning:

```text
bronze/events/
processing_date=<YYYY-MM-DD>/
topic=<kafka-topic>/
partition=<kafka-partition>/
offsets=<start-offset>-<end-offset>.jsonl
```

`processing_date` is derived from the timezone-aware UTC `ingestion_time`.

It is not derived from:

- `event_time`;
- `producer_time`;
- the Windows local timezone;
- the Kafka record timestamp.

This layout supports operational filtering and replay by the date on which the Bronze pipeline ingested the data.

The `key=value` path segments are object-key prefixes. Object storage has a flat key namespace; consoles present slash-delimited prefixes as folder-like navigation.

---

## Object-key policy

An object key is deterministic for the same:

```text
processing_date
+ topic
+ partition
+ start_offset
+ end_offset
```

Example:

```text
bronze/events/
processing_date=2026-07-13/
topic=retail-payment-events/
partition=00002/
offsets=00000000000000000320-00000000000000000351.jsonl
```

`ingestion_run_id` is intentionally not a physical partition component. It remains available in each row, in object metadata, and in structured logs. This avoids creating a high-cardinality prefix for every ingestion execution.

### Cross-date retry limitation

If an uncommitted Kafka source range is retried on a different UTC date, the same offsets can be written under a different `processing_date`. This is allowed by the current at-least-once design. Downstream reconciliation and deduplication must use source coordinates rather than object path alone.

---

## Object metadata

Each object stores technical metadata for fast inspection without parsing every JSONL row.

| Metadata key | Meaning |
|---|---|
| `sha256` | SHA-256 calculated from the serialized object body before upload |
| `record-count` | Number of JSONL rows |
| `record-version` | Bronze envelope version |
| `source-topic` | Kafka source topic |
| `source-partition` | Kafka source partition |
| `start-offset` | First source offset in the object |
| `end-offset` | Last source offset in the object |
| `processing-date` | UTC processing date |
| `ingestion-run-id` | Ingestion execution identity |
| `ingestion-batch-id` | Run-scoped batch identity |
| `ingestion-time` | UTC ingestion timestamp |

`HeadObject` can verify the object size and returned metadata without downloading the body. Comparing a custom SHA-256 metadata value only verifies that the expected metadata was stored; a full stored-body checksum verification requires reading the body or using a supported object-store checksum response.

---

## Storage invariants

For every Bronze object:

1. All rows have the same `source_topic`.
2. All rows have the same `source_partition`.
3. Source offsets increase within the object.
4. The first and last row offsets match the object-key range.
5. `record-count` matches the number of JSONL rows.
6. The object-key `processing_date`, object metadata `processing-date`, and row-level `processing_date` match.
7. Raw key/value bytes can be reconstructed from Base64.
8. Offset commit uses `end_offset + 1`.
9. Duplicate and malformed source records remain present.

---

## Non-goals

Bronze does not:

- validate business rules;
- reject negative amounts;
- reject unsupported schema versions;
- deduplicate `event_id`;
- classify DLQ records;
- correct timestamps;
- calculate business metrics;
- modify raw Kafka key or value bytes;
- provide end-to-end exactly-once delivery.

These responsibilities belong to Silver, Gold, or later reliability hardening.

---

## Historical development objects

Objects produced before processing-date partitioning remain under:

```text
bronze/events/_unpartitioned/
```

Some earlier objects also use an older additive Bronze envelope without all ingestion metadata fields.

These objects are development evidence. They should be excluded from processing-date replay and can be removed after:

1. the partitioned path has been verified;
2. a clean final Week 2 ingestion has been recorded;
3. required evidence has been retained.

---

## Verified Day 3 reconciliation

| Partition | Offset coverage | Records | Objects | Kafka lag |
|---:|---|---:|---:|---:|
| 0 | `0–383` | 384 | 5 | 0 |
| 1 | `0–299` | 300 | 4 | 0 |
| 2 | `0–319` | 320 | 4 | 0 |
| **Total** | — | **1004** | **13** | **0** |

At the verification point:

```text
Kafka records = 1004
Bronze JSONL rows = 1004
missing source offsets = 0
overlapping source ranges = 0
```

---

## Current limitations

- Historical `_unpartitioned` objects use older development layouts.
- Full stored-body checksum verification is not performed on every production-path write.
- Replay by `processing_date` is implemented in the next step.
- Rebalance-aware flushing and broader failure-injection testing are deferred to hardening.
- Cross-date retries can create duplicate source ranges in different processing-date prefixes.
- PostgreSQL ingestion-run metadata is deferred to the metadata hardening phase.

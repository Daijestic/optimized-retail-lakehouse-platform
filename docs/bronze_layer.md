# Bronze Layer

## Purpose

Bronze preserves Kafka records for audit, replay, debugging and
downstream reprocessing.

## Guarantees

- Kafka key and value bytes are preserved using Base64.
- Source topic, partition and offset are retained.
- Malformed and business-invalid records are not dropped.
- Kafka offsets are committed only after object persistence succeeds.
- Each ingestion execution has one ingestion_run_id.

## Record fields

| Field | Type | Meaning |
|---|---|---|
| record_version | string | Bronze envelope version |
| ingestion_run_id | string | One ingestion execution |
| ingestion_batch_number | integer | Batch number inside the run |
| ingestion_batch_id | string | Run-scoped batch identity |
| ingestion_time | UTC timestamp | Time Bronze envelope was created |
| source_record_id | string | topic:partition:offset |
| source_topic | string | Kafka topic |
| source_partition | integer | Kafka partition |
| source_offset | integer | Kafka offset |
| kafka_timestamp_ms | integer/null | Kafka record timestamp |
| key_base64 | string/null | Original Kafka key bytes |
| value_base64 | string/null | Original Kafka value bytes |
| payload_parse_status | string | Best-effort JSON parsing status |
| event_id | string/null | Best-effort extracted field |
| event_type | string/null | Best-effort extracted field |
| event_time | string/null | Best-effort extracted field |
| producer_time | string/null | Best-effort extracted field |
| schema_version | string/null | Best-effort extracted field |

## Parse status

- parsed_object
- invalid_json
- invalid_utf8
- json_not_object
- null_payload

## Non-goals

Bronze does not:

- validate business rules;
- reject negative amount;
- reject unsupported schema version;
- deduplicate event_id;
- classify DLQ records;
- correct timestamps;
- modify raw payload.
import json
from pathlib import Path

from producer.schemas import RetailPaymentEvent


OUTPUT_PATH = Path(
    "docs/generated/retail_payment_event_v1.schema.json"
)


def main() -> None:
    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    schema = RetailPaymentEvent.model_json_schema()

    OUTPUT_PATH.write_text(
        json.dumps(schema, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"Schema exported to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
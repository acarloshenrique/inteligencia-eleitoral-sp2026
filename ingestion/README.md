Ingestion layer.

- Accepts files exactly as delivered by source systems.
- Must not normalize schema, encoding, dates, or business keys.
- Upstream pipelines copy or land inputs here before bronze/silver/gold processing.

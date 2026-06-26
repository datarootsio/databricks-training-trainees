# Day 2 — Landing zone (ADLS Gen2, Parquet)

The Day 2 Lakeflow pipeline ingests **Parquet** files with Auto Loader from an
**ADLS Gen2 landing zone**, surfaced in Unity Catalog as an **external Volume**. This
folder holds the script that builds that landing zone from the raw Olist CSVs.

## 1. Build the Parquet landing zone

```bash
pip install duckdb            # or: uv add duckdb
python prepare_landing.py --src /path/to/olist_csvs --out ./landing
```

This writes one Parquet file per source table into `landing/<table>/<table>.parquet`,
matching the sub-folders the pipeline reads (`orders/`, `customers/`, `order_items/`,
`products/`, `sellers/`, `order_payments/`, `order_reviews/`, `translation/`).

> **Why all columns are stored as strings:** the landing data is intentionally raw and
> untyped. Bronze ingests it as-is; the **Silver** layer owns casting and typing. Switching
> CSV → Parquet changes only the file format, not this discipline.

## 2. Stand up the ADLS Gen2 landing zone in Unity Catalog

One-time setup (admin), done with the trainer:

1. **Storage credential** → a UC storage credential backed by an Azure managed identity /
   service principal that can read the container.
   ```sql
   CREATE STORAGE CREDENTIAL olist_cred
     WITH AZURE_MANAGED_IDENTITY ...;   -- or service principal
   ```
2. **External location** → point UC at the ADLS Gen2 path:
   ```sql
   CREATE EXTERNAL LOCATION olist_landing
     URL 'abfss://landing@<storage_account>.dfs.core.windows.net/olist/'
     WITH (STORAGE CREDENTIAL olist_cred);
   ```
3. **External Volume** → expose it as a Volume the pipeline can read by path:
   ```sql
   CREATE EXTERNAL VOLUME <catalog>.<schema>.landing
     LOCATION 'abfss://landing@<storage_account>.dfs.core.windows.net/olist/';
   ```
   The pipeline then reads `/Volumes/<catalog>/<schema>/landing/<table>/`.

## 3. Upload the Parquet files

Copy `landing/` into the container so the layout becomes:

```
abfss://landing@<storage_account>.dfs.core.windows.net/olist/
  orders/orders.parquet
  customers/customers.parquet
  ... (one folder per table)
```

e.g. with `azcopy`:
```bash
azcopy copy "./landing/*" "https://<storage_account>.blob.core.windows.net/landing/olist/" --recursive
```

## 4. How the pipeline reads it

SQL (Lakeflow editor):
```sql
CREATE OR REFRESH STREAMING TABLE brz_orders AS
SELECT *, current_timestamp() AS _ingested_at
FROM STREAM read_files(
  "/Volumes/<catalog>/<schema>/<volume>/orders/",
  format => "parquet"
);
```

Python (Auto Loader):
```python
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .load("/Volumes/<catalog>/<schema>/<volume>/orders/")
```

Parquet carries its own schema, so there is no `header` option (unlike CSV). To see
incremental ingestion in action, upload a table's files in two batches and re-run the
pipeline — Auto Loader picks up only the new files.

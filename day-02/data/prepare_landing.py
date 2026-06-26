#!/usr/bin/env python3
"""
Prepare the Olist landing zone as PARQUET for the Day 2 Lakeflow pipeline.

The Day 2 pipeline ingests Parquet from an ADLS Gen2 landing zone (surfaced in
Unity Catalog as an external Volume). This script converts the raw Olist CSVs
(from Kaggle: olistbr/brazilian-ecommerce) into one Parquet file per source
table, laid out in the sub-folders the pipeline expects:

    <landing>/orders/orders.parquet
    <landing>/customers/customers.parquet
    <landing>/order_items/order_items.parquet
    <landing>/products/products.parquet
    <landing>/sellers/sellers.parquet
    <landing>/order_payments/order_payments.parquet
    <landing>/order_reviews/order_reviews.parquet
    <landing>/translation/translation.parquet

Design choice: every column is written as STRING (raw), exactly as it appears
in the CSV. This keeps the teaching intact — Bronze ingests raw, untyped data;
the Silver layer is where casting/typing happens. The file format changes
(CSV -> Parquet); the schema discipline does not.

Usage:
    pip install duckdb            # or: uv add duckdb
    python prepare_landing.py --src /path/to/olist_csvs --out ./landing

Then upload <landing>/ to your ADLS Gen2 container (see data/README.md) so the
pipeline can read /Volumes/<catalog>/<schema>/<volume>/<table>/.
"""
import argparse, os, sys
import duckdb

# Kaggle CSV filename  ->  landing sub-folder (matches the pipeline's read_files paths)
TABLES = {
    "olist_orders_dataset.csv":                 "orders",
    "olist_customers_dataset.csv":              "customers",
    "olist_order_items_dataset.csv":            "order_items",
    "olist_products_dataset.csv":               "products",
    "olist_sellers_dataset.csv":                "sellers",
    "olist_order_payments_dataset.csv":         "order_payments",
    "olist_order_reviews_dataset.csv":          "order_reviews",
    "product_category_name_translation.csv":    "translation",
}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="folder containing the Olist *.csv files")
    ap.add_argument("--out", default="./landing", help="output landing folder (default ./landing)")
    args = ap.parse_args()

    con = duckdb.connect()
    missing = [f for f in TABLES if not os.path.exists(os.path.join(args.src, f))]
    if missing:
        sys.exit(f"Missing CSVs in {args.src}: {', '.join(missing)}")

    for csv, folder in TABLES.items():
        dest_dir = os.path.join(args.out, folder)
        os.makedirs(dest_dir, exist_ok=True)
        src = os.path.join(args.src, csv).replace("'", "''")
        dest = os.path.join(dest_dir, f"{folder}.parquet").replace("'", "''")
        # all_varchar=true -> keep every column as raw string (Bronze stays untyped)
        con.execute(
            f"COPY (SELECT * FROM read_csv_auto('{src}', header=true, all_varchar=true)) "
            f"TO '{dest}' (FORMAT parquet)"
        )
        n = con.execute(f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()[0]
        print(f"  {folder:16} {n:>8,} rows -> {dest}")

    print(f"\nDone. Landing zone ready at: {os.path.abspath(args.out)}")
    print("Next: upload it to ADLS Gen2 — see day-02/data/README.md")

if __name__ == "__main__":
    main()

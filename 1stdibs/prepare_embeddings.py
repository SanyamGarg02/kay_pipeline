import pandas as pd
import json
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
import re
import argparse
import os
from dotenv import load_dotenv

# --------------------------
# Load environment variables
# --------------------------
load_dotenv()  # loads variables from .env

# --------------------------
# Helper functions
# --------------------------
def normalize_price(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    try:
        cleaned = re.sub(r"[^\d.]", "", str(val))
        return int(float(cleaned))
    except Exception:
        return None

def normalize_gold_karat(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    try:
        return int(re.sub(r"[^0-9]", "", str(val)))
    except Exception:
        return None

def normalize_ring_size(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    val = str(val).strip()
    try:
        return float(re.sub(r"[^\d.]", "", val))
    except Exception:
        return None

def safe_str(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    return str(val).strip()

def build_text(row):
    parts = [
        safe_str(row.get("name")),
        f"{safe_str(row.get('stone_type'))} {safe_str(row.get('stone_shape'))} "
        f"Clarity {safe_str(row.get('stone_clarity'))} Color {safe_str(row.get('stone_color'))} "
        f"{row.get('stone_carat_weight')} Carat",
        f"Metal: {safe_str(row.get('metal_type'))} {safe_str(row.get('metal_color'))} {row.get('gold_karat')}",
        f"Ring Size: {row.get('ring_size')}",
        f"Category: {safe_str(row.get('category'))}",
        f"Source: {safe_str(row.get('source'))}"
    ]
    return " | ".join([p for p in parts if p and str(p).strip()])

# --------------------------
# Main
# --------------------------
def main(args):
    print("Loading embedding model...")
    model = SentenceTransformer(args.model)

    print(f"Loading CSV: {args.input}")
    df = pd.read_csv(args.input)

    # Drop rows missing name/price
    initial_count = len(df)
    df = df.dropna(subset=["name", "price"])
    dropped_count = initial_count - len(df)
    print(f"⚠️ Dropped {dropped_count} rows due to missing name or price.")
    print(f"✅ Remaining {len(df)} rows will be uploaded.")

    print("Connecting to Elasticsearch...")

    es_host = args.host or os.getenv("ES_HOST")
    print("Elasticsearch host:", es_host)
    es_user = args.user or os.getenv("ES_USER")
    print("Elasticsearch user:", es_user)
    es_password = args.password or os.getenv("ES_PASSWORD")
    print("Elasticsearch password:", es_password)

    if not es_host or not es_user or not es_password:
        raise ValueError("Elasticsearch host/user/password not set. Provide via CLI or in .env")

    es = Elasticsearch(
        es_host,
        basic_auth=(es_user, es_password),
        verify_certs=False
    )

    try:
        info = es.info()
        print("✅ Connected to Elasticsearch:", info["cluster_name"])
    except Exception as e:
        raise RuntimeError(f"❌ Could not connect to Elasticsearch! {e}")


    actions = []
    uploaded_count = 0

    with open(args.output, "w", encoding="utf-8") as f_ndjson:
        for idx, row in tqdm(df.iterrows(), total=len(df)):
            doc_id = f"1stdibs-{idx + 1}"
            text = build_text(row)
            embedding = model.encode(text, normalize_embeddings=True).tolist()

            doc = {
                "name": safe_str(row.get("name")),
                "price": normalize_price(row.get("price")),
                "url": safe_str(row.get("url")),
                "stone_type": safe_str(row.get("stone_type")),
                "stone_shape": safe_str(row.get("stone_shape")),
                "stone_clarity": safe_str(row.get("stone_clarity")),
                "stone_color": safe_str(row.get("stone_color")),
                "stone_carat_weight": row.get("stone_carat_weight") if not pd.isna(row.get("stone_carat_weight")) else None,
                "metal_type": safe_str(row.get("metal_type")),
                "metal_color": safe_str(row.get("metal_color")),
                "gold_karat": normalize_gold_karat(row.get("gold_karat")),
                "ring_size": normalize_ring_size(row.get("ring_size")),
                "category": safe_str(row.get("category")),
                "source": safe_str(row.get("source")),
                "embedding": embedding
            }

            action = {
                "_op_type": "update",
                "_index": args.index,
                "_id": doc_id,
                "doc": doc,
                "doc_as_upsert": True
            }
            actions.append(action)
            uploaded_count += 1

            # Write NDJSON for backup
            f_ndjson.write(json.dumps({"update": {"_index": args.index, "_id": doc_id}}) + "\n")
            f_ndjson.write(json.dumps({"doc": doc, "doc_as_upsert": True}) + "\n")

    print("Uploading to Elasticsearch...")
    try:
        helpers.bulk(es, actions)
        print(f"✅ All {uploaded_count} 1stdibs documents upserted successfully!")
        print(f"⚠️ {dropped_count} rows were dropped due to missing name or price.")
    except BulkIndexError as e:
        failed_count = len(e.errors)
        print(f"❌ {failed_count} documents failed to upsert.")
        with open(f"{args.output}_failed.json", "w", encoding="utf-8") as f:
            json.dump(e.errors, f, indent=2)
        print("💾 Saved failed docs for inspection.")

# --------------------------
# CLI
# --------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare 1stdibs embeddings and upload to Elasticsearch")

    parser.add_argument("--input", default="1stdibs/poc_1stdibs_normalized.csv", help="Input normalized CSV file")
    parser.add_argument(
    "--output",
    required=True,
    help="Output NDJSON file (pipeline should provide full path)"
)

    parser.add_argument("--index", default="competitor_offers_test", help="Elasticsearch index name")
    parser.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2", help="SentenceTransformer model")
    parser.add_argument("--host", default=None, help="Elasticsearch host URL (optional, fallback to .env)")
    parser.add_argument("--user", default=None, help="Elasticsearch username (optional, fallback to .env)")
    parser.add_argument("--password", default=None, help="Elasticsearch password (optional, fallback to .env)")

    args = parser.parse_args()
    main(args)

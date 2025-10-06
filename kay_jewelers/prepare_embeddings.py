import pandas as pd
import json
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
import re
from fractions import Fraction
import math
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
def normalize_stone_carat(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    val = str(val).strip().replace("–", "-").replace("—", "-")
    try:
        if "-" in val:
            parts = val.split("-")
            whole = float(parts[0])
            frac = float(Fraction(parts[1]))
            return whole + frac
        if "/" in val:
            return float(Fraction(val))
        return float(re.sub("[^0-9.]", "", val))
    except Exception:
        return None

def normalize_gold_karat(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    try:
        return int(re.sub("[^0-9]", "", str(val)))
    except Exception:
        return None

def normalize_ring_size(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    try:
        return float(re.sub(r"[^\d.]", "", str(val)))
    except Exception:
        return None

def safe_str(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    return str(val).strip()

def nan_to_none(val):
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    return val

def build_text(row):
    parts = [
        safe_str(row.get("name")),
        f"{safe_str(row.get('stone_type'))} {safe_str(row.get('stone_shape'))} "
        f"Clarity {safe_str(row.get('stone_clarity'))} Color {safe_str(row.get('stone_color'))} "
        f"{row.get('stone_carat_weight')} Carat",
        f"Metal: {safe_str(row.get('metal_type'))} {safe_str(row.get('metal_color'))} {row.get('gold_karat')}",
        f"Ring Size: {row.get('ring_size')}",
        f"Source: {safe_str(row.get('source'))}"
    ]
    return " | ".join([p for p in parts if p and str(p).strip()])

def infer_category(row):
    name = (row.get("name") or "").lower()
    url = (row.get("url") or "").lower()
    text = f"{name} {url}"

    if any(word in text for word in ["earring", "stud", "hoop", "drop"]):
        return "Earring"
    elif any(word in text for word in ["bracelet", "bangle", "cuff"]):
        return "Bracelet"
    elif any(word in text for word in ["necklace", "pendant", "chain"]):
        return "Necklace"
    elif any(word in text for word in ["ring", "engagement", "band"]):
        return "Ring"
    else:
        return "Unknown"

# --------------------------
# Main
# --------------------------
def main(args):
    print("Loading embedding model...")
    model = SentenceTransformer(args.model)

    print(f"Loading CSV: {args.input}")
    df = pd.read_csv(args.input)

    df["stone_carat_weight"] = df["stone_carat_weight"].apply(normalize_stone_carat)
    df["gold_karat"] = df["gold_karat"].apply(normalize_gold_karat)
    df["ring_size"] = df["ring_size"].apply(normalize_ring_size)
    df["price"] = df["price"].apply(lambda x: nan_to_none(x))

    print("Connecting to Elasticsearch...")

    es_host = args.host or os.getenv("ES_HOST")
    es_user = args.user or os.getenv("ES_USER")
    es_password = args.password or os.getenv("ES_PASSWORD")

    if not es_host or not es_user or not es_password:
        raise ValueError("Elasticsearch host/user/password not set. Provide via CLI or in .env")

    es = Elasticsearch(
        es_host,
        basic_auth=(es_user, es_password),
        verify_certs=False
    )

    if not es.ping():
        raise RuntimeError("❌ Could not connect to Elasticsearch!")
    print("✅ Connected to Elasticsearch")

    actions = []
    with open(args.output, "w", encoding="utf-8") as f_ndjson:
        for idx, row in tqdm(df.iterrows(), total=len(df)):
            doc_id = str(idx + 1)
            text = build_text(row)
            embedding = model.encode(text, normalize_embeddings=True).tolist()

            doc = {
                "name": safe_str(row.get("name")),
                "price": nan_to_none(row.get("price")),
                "url": safe_str(row.get("url")),
                "stone_type": safe_str(row.get("stone_type")),
                "stone_shape": safe_str(row.get("stone_shape")),
                "stone_clarity": safe_str(row.get("stone_clarity")),
                "stone_color": safe_str(row.get("stone_color")),
                "stone_carat_weight": nan_to_none(row.get("stone_carat_weight")),
                "metal_type": safe_str(row.get("metal_type")),
                "metal_color": safe_str(row.get("metal_color")),
                "gold_karat": nan_to_none(row.get("gold_karat")),
                "ring_size": nan_to_none(row.get("ring_size")),
                "category": infer_category(row),
                "source": safe_str(row.get("source")),
                "embedding": embedding,
            }

            action = {
                "_op_type": "index",
                "_index": args.index,
                "_id": doc_id,
                "_source": doc
            }
            actions.append(action)

            f_ndjson.write(json.dumps({"index": {"_index": args.index, "_id": doc_id}}) + "\n")
            f_ndjson.write(json.dumps(doc) + "\n")

    print("Uploading to Elasticsearch...")
    try:
        helpers.bulk(es, actions)
        print("✅ All documents uploaded successfully!")
    except BulkIndexError as e:
        failed_count = len(e.errors)
        print(f"❌ {failed_count} documents failed to index.")
        with open("failed_docs.json", "w", encoding="utf-8") as f:
            json.dump(e.errors, f, indent=2)
        print("💾 Saved failed docs for inspection.")

# --------------------------
# CLI
# --------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare embeddings and upload to Elasticsearch")

    parser.add_argument("--input", default="kay_jewelers/poc_kay_normalized.csv", help="Input normalized CSV file")
    parser.add_argument(
    "--output",
    required=True,
    help="Output NDJSON file (pipeline should provide full path)"
)
    parser.add_argument("--index", default="competitor_offers_test", help="Elasticsearch index name")  #change as per your index name or add in command bia cli
    parser.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2", help="SentenceTransformer model")
    parser.add_argument("--host", default=None, help="Elasticsearch host URL (optional, fallback to .env)")
    parser.add_argument("--user", default=None, help="Elasticsearch username (optional, fallback to .env)")
    parser.add_argument("--password", default=None, help="Elasticsearch password (optional, fallback to .env)")

    args = parser.parse_args()
    main(args)

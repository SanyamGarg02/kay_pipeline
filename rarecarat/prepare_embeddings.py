import pandas as pd
import json
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
import re
import math
import os

# --------------------------
# Config
# --------------------------
INPUT_CSV = "rarecarat/poc_rarecarat_normalized.csv"
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

ES_HOST = "https://localhost:9200"
ES_USER = "elastic"
ES_PASS = "TdBXFT++ygKRs-TLqBbp"  # <-- update
ES_INDEX = "competitor_offers"

NDJSON_FILE = "rarecarat/competitor_offers_rarecarat.ndjson"

# --------------------------
# Load embedding model
# --------------------------
print("Loading embedding model...")
model = SentenceTransformer(MODEL_NAME)

# --------------------------
# Load CSV
# --------------------------
print(f"Loading CSV: {INPUT_CSV}")
df = pd.read_csv(INPUT_CSV)

# --------------------------
# Filter out rows missing name or price
# --------------------------
initial_count = len(df)
df = df.dropna(subset=["name", "price"])
dropped_count = initial_count - len(df)
print(f"⚠️ Dropped {dropped_count} rows due to missing name or price.")
print(f"✅ Remaining {len(df)} rows will be uploaded.")

# --------------------------
# Helper functions
# --------------------------
def normalize_price(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    try:
        cleaned = re.sub(r"[^\d.]", "", str(val))
        return int(float(cleaned))  # <-- cast to int
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
        if "to" in val:
            parts = [float(x) for x in re.findall(r"[\d.]+", val)]
            return int(round(sum(parts)/len(parts)))  # average of range
        else:
            return int(round(float(val)))
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
# Connect to Elasticsearch
# --------------------------
print("Connecting to Elasticsearch...")
es = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USER, ES_PASS),
    verify_certs=False
)

if not es.ping():
    raise RuntimeError("❌ Could not connect to Elasticsearch!")
print("✅ Connected to Elasticsearch")

# --------------------------
# Prepare actions
# --------------------------
actions = []
uploaded_count = 0

with open(NDJSON_FILE, "w", encoding="utf-8") as f_ndjson:
    for idx, row in tqdm(df.iterrows(), total=len(df)):
        doc_id = f"rarecarat-{idx + 1}"
        text = build_text(row)
        embedding = model.encode(text, normalize_embeddings=True).tolist()

        doc = {
            "name": safe_str(row.get("name")),
            "price": normalize_price(row.get("price")),
            "url": safe_str(row.get("url")),
            "stone_type": "Diamond",  # all products
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
            "_op_type": "update",  # upsert
            "_index": ES_INDEX,
            "_id": doc_id,
            "doc": doc,
            "doc_as_upsert": True
        }
        actions.append(action)
        uploaded_count += 1

        # Write NDJSON for backup
        f_ndjson.write(json.dumps({"update": {"_index": ES_INDEX, "_id": doc_id}}) + "\n")
        f_ndjson.write(json.dumps({"doc": doc, "doc_as_upsert": True}) + "\n")

# --------------------------
# Bulk upsert
# --------------------------
print("Uploading to Elasticsearch...")
try:
    helpers.bulk(es, actions)
    print(f"✅ All {uploaded_count} RareCarat documents upserted successfully!")
    print(f"⚠️ {dropped_count} rows were dropped due to missing name or price.")
except BulkIndexError as e:
    failed_count = len(e.errors)
    print(f"❌ {failed_count} documents failed to upsert.")
    with open("rarecarat/failed_rarecarat_docs.json", "w", encoding="utf-8") as f:
        json.dump(e.errors, f, indent=2)
    print("💾 Saved failed docs for inspection.")

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
INPUT_CSV = "glamira/poc_glamira_normalized.csv"  # <-- your Glamira file
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

ES_HOST = "https://localhost:9200"
ES_USER = "elastic"
ES_PASS = "TdBXFT++ygKRs-TLqBbp"  # <-- update this
ES_INDEX = "competitor_offers"

NDJSON_FILE = "glamira/competitor_offers_glamira.ndjson"

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
# Helper functions
# --------------------------
def normalize_price(val):
    """Convert price string like '$28,392.00' → 28392 (int) or None"""
    if pd.isna(val) or str(val).strip() == "":
        return None
    try:
        cleaned = re.sub(r"[^\d.]", "", str(val))
        return int(float(cleaned))
    except Exception:
        return None

def normalize_gold_karat(val):
    """Convert gold karat like '14K' → 14 (int) or None"""
    if pd.isna(val) or str(val).strip() == "":
        return None
    try:
        return int(re.sub(r"[^0-9]", "", str(val)))
    except Exception:
        return None

def normalize_ring_size(val):
    """Convert ring size, handle 'Click to edit' → 7, empty → None"""
    if pd.isna(val) or str(val).strip() == "":
        return None
    val = str(val).strip()
    if val.lower() == "click to edit":
        return 7.0
    try:
        return float(re.sub(r"[^\d.]", "", val))
    except Exception:
        return None

def safe_str(val):
    """Return stripped string or None"""
    if pd.isna(val) or str(val).strip() == "":
        return None
    return str(val).strip()

def build_text(row):
    """Build a descriptive text for embeddings"""
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

# --------------------------
# Connect to Elasticsearch
# --------------------------
print("Connecting to Elasticsearch...")

ca_cert_path = "/path/to/http_ca.crt"  # <-- update if you have it
if os.path.exists(ca_cert_path):
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASS),
        verify_certs=False,
        
    )
else:
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

with open(NDJSON_FILE, "w", encoding="utf-8") as f_ndjson:
    for idx, row in tqdm(df.iterrows(), total=len(df)):
        doc_id = f"glamira-{idx + 1}"
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

        # Write NDJSON for backup
        f_ndjson.write(json.dumps({"update": {"_index": ES_INDEX, "_id": doc_id}}) + "\n")
        f_ndjson.write(json.dumps({"doc": doc, "doc_as_upsert": True}) + "\n")

# --------------------------
# Bulk upsert
# --------------------------
print("Uploading to Elasticsearch...")
try:
    helpers.bulk(es, actions)
    print("✅ All Glamira documents upserted successfully!")
except BulkIndexError as e:
    failed_count = len(e.errors)
    print(f"❌ {failed_count} documents failed to upsert.")
    with open("glamira/failed_glamira_docs.json", "w", encoding="utf-8") as f:
        json.dump(e.errors, f, indent=2)
    print("💾 Saved failed docs for inspection.")

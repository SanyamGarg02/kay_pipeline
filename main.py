# # gemgem/main.py
# import os
# import pandas as pd
# import json
# import re
# import ast
# import math
# import streamlit as st
# from sentence_transformers import SentenceTransformer
# from elasticsearch import Elasticsearch
# from typing import Any, Dict, Optional
# from dotenv import load_dotenv

# # --------------------------
# # Config via .env
# # --------------------------
# load_dotenv()

# MODEL_NAME = os.getenv("MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
# INDEX_NAME = os.getenv("INDEX_NAME", "competitor_offers")
# ES_HOST = os.getenv("ES_HOST", "https://localhost:9200")
# ES_USER = os.getenv("ES_USER", "elastic")
# ES_PASS = os.getenv("ES_PASSWORD")

# if not ES_PASS:
#     raise RuntimeError("Elasticsearch password not set. Please add ES_PASS to .env")

# # --------------------------
# # Caching resources
# # --------------------------
# @st.cache_resource
# def load_model():
#     return SentenceTransformer(MODEL_NAME)

# @st.cache_resource
# def connect_es():
#     return Elasticsearch(
#         ES_HOST,
#         basic_auth=(ES_USER, ES_PASS),
#         verify_certs=False,   # ignore SSL verification
#         ssl_show_warn=False   # suppress SSL warnings
#     )

# model = load_model()
# es = connect_es()

# # --------------------------
# # Utility parsing helpers
# # --------------------------
# def safe_float(val):
#     if val is None:
#         return None
#     try:
#         f = float(val)
#         if math.isnan(f):
#             return None
#         return f
#     except Exception:
#         return None

# def parse_fraction_or_float(value: Any) -> Optional[float]:
#     if value is None:
#         return None
#     s = str(value).strip()
#     if s == "":
#         return None
#     s = s.replace("–", "-").replace("—", "-").replace(" ", "")
#     m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(ctw|ct|carat)?", s, flags=re.IGNORECASE)
#     if m and m.group(1):
#         try:
#             return float(m.group(1))
#         except:
#             pass
#     if "-" in s or " " in s:
#         sep = "-" if "-" in s else " "
#         parts = s.split(sep)
#         try:
#             whole = float(parts[0])
#             frac = parts[1]
#             if "/" in frac:
#                 num, den = frac.split("/")
#                 return whole + float(num) / float(den)
#             return whole + float(frac)
#         except Exception:
#             pass
#     if "/" in s:
#         try:
#             num, den = s.split("/")
#             return float(num) / float(den)
#         except:
#             pass
#     num = re.sub(r"[^0-9.]", "", s)
#     return safe_float(num)

# def normalize_gold_karat(value: Any) -> Optional[int]:
#     if value is None:
#         return None
#     s = str(value)
#     m = re.search(r"(\d{1,2})\s*[Kk]", s)
#     if m:
#         try:
#             return int(m.group(1))
#         except:
#             pass
#     digits = re.sub(r"[^0-9]", "", s)
#     if digits:
#         try:
#             return int(digits)
#         except:
#             pass
#     return None

# def extract_metal_type_and_color(s: str):
#     if not s:
#         return None, None
#     s_low = s.lower()
#     metal_type = None
#     metal_color = None
#     if "gold" in s_low:
#         metal_type = "Gold"
#     elif "silver" in s_low or "sterling" in s_low:
#         metal_type = "Silver"
#     elif "platinum" in s_low:
#         metal_type = "Platinum"
#     if "white" in s_low:
#         metal_color = "White"
#     elif "yellow" in s_low:
#         metal_color = "Yellow"
#     elif "rose" in s_low or "rose gold" in s_low:
#         metal_color = "Rose"
#     return metal_type, metal_color

# def detect_stone_type_from_text(s: str) -> Optional[str]:
#     if not s:
#         return None
#     s_low = s.lower()
#     for stone in ["diamond", "ruby", "sapphire", "emerald", "opal", "topaz", "amethyst"]:
#         if stone in s_low:
#             return stone.capitalize()
#     return None

# def detect_shape_from_text(s: str) -> Optional[str]:
#     if not s:
#         return None
#     s_low = s.lower()
#     for shp in ["round", "princess", "pear", "oval", "emerald", "marquise", "cushion", "asscher", "radiant"]:
#         if shp in s_low:
#             return shp.capitalize()
#     return None

# def try_parse_details_json(raw):
#     if raw is None:
#         return {}
#     if isinstance(raw, dict):
#         return raw
#     text = str(raw).strip()
#     for attempt in (json.loads, ast.literal_eval):
#         try:
#             parsed = attempt(text)
#             if isinstance(parsed, dict):
#                 return parsed
#         except Exception:
#             pass
#     cleaned = text.replace('""', '"')
#     try:
#         parsed = json.loads(cleaned)
#         if isinstance(parsed, dict):
#             return parsed
#     except:
#         pass
#     d = {}
#     for m in re.finditer(r'"?([^":]+)"?\s*:\s*"?([^",}]+)"?', text):
#         k = m.group(1).strip()
#         v = m.group(2).strip()
#         d[k] = v
#     return d

# # --------------------------
# # Robust gemgem normalizer
# # --------------------------
# def normalize_gemgem_details(details_raw: Any, name: str, price: Any) -> Dict[str, Any]:
#     parsed = try_parse_details_json(details_raw)
#     stone_section = None
#     metal_section = None
#     specs_section = None
#     stone_keys = ["Stone(s)", "Stones", "stone(s)", "stone", "Stone"]
#     metal_keys = ["Metal(s)", "Metals", "metal(s)", "metal", "Metal"]
#     specs_keys = ["Specifications", "Specs", "specifications", "specs"]

#     if isinstance(parsed, dict):
#         for k in parsed.keys():
#             if any(sk.lower() == k.lower() for sk in stone_keys):
#                 stone_section = parsed[k]
#             if any(mk.lower() == k.lower() for mk in metal_keys):
#                 metal_section = parsed[k]
#             if any(pk.lower() == k.lower() for pk in specs_keys):
#                 specs_section = parsed[k]

#     def g(dict_or_none, *candidates):
#         if not dict_or_none:
#             return None
#         if isinstance(dict_or_none, dict):
#             for cand in candidates:
#                 for key in dict_or_none.keys():
#                     if key and cand.lower().replace(" ", "") == str(key).lower().replace(" ", ""):
#                         return dict_or_none.get(key)
#         return None

#     stone_type = g(stone_section, "Stone Type", "stone type", "stone")
#     stone_shape = g(stone_section, "Diamond Shape", "Shape", "diamond shape")
#     stone_clarity = g(stone_section, "Clarity")
#     stone_color = g(stone_section, "Color")
#     carat_raw = g(stone_section, "Carat Weight", "Carat")

#     metal_full = None
#     if isinstance(metal_section, dict):
#         metal_full = g(metal_section, "Metal", "Metal Type")
#     elif isinstance(metal_section, str):
#         metal_full = metal_section

#     gold_karat = normalize_gold_karat(g(metal_section, "Metal") or metal_full)
#     metal_type, metal_color = extract_metal_type_and_color(metal_full or "")

#     size_val = g(specs_section, "Size", "Length")
#     ring_size = None
#     if size_val:
#         s = str(size_val).lower()
#         if "cm" in s or "mm" in s or "in" in s:
#             ring_size = None
#         else:
#             ring_size = parse_fraction_or_float(s)

#     if carat_raw is None:
#         m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(ctw|ct|carat)?", name, flags=re.IGNORECASE)
#         if m and m.group(1):
#             try:
#                 carat_val = float(m.group(1))
#             except:
#                 carat_val = None
#         else:
#             carat_val = None
#     else:
#         carat_val = parse_fraction_or_float(carat_raw)

#     if not stone_type:
#         stone_type = detect_stone_type_from_text(name)
#     if not stone_shape:
#         stone_shape = detect_shape_from_text(name)

#     price_val = safe_float(price)

#     normalized = {
#         "name": name,
#         "price": price_val,
#         "stone_type": stone_type,
#         "stone_shape": stone_shape,
#         "stone_clarity": stone_clarity,
#         "stone_color": stone_color,
#         "stone_carat_weight": carat_val,
#         "metal_type": metal_type,
#         "metal_color": metal_color,
#         "gold_karat": gold_karat,
#         "ring_size": ring_size,
#         "source": "GemGem"
#     }
#     return normalized

# def build_text_from_normalized(n):
#     parts = [
#         n.get("name") or "",
#         f"{n.get('stone_type') or ''} {n.get('stone_shape') or ''} Clarity {n.get('stone_clarity') or ''} Color {n.get('stone_color') or ''} {n.get('stone_carat_weight') or ''} Carat",
#         f"Metal: {n.get('metal_type') or ''} {n.get('metal_color') or ''} {n.get('gold_karat') or ''}",
#         f"Ring Size: {n.get('ring_size') or ''}",
#         f"Source: {n.get('source') or ''}"
#     ]
#     return " | ".join([p for p in parts if p and str(p).strip()])

# # -------------------------------
# # Search with filters + safeguard
# # -------------------------------
# def search_similar(es, embedding, normalized, tolerance, k=10):
#     stone_type = normalized.get("stone_type")
#     carat = normalized.get("stone_carat_weight")
#     gold_karat = normalized.get("gold_karat")

#     filters = []
#     if stone_type:
#         filters.append({"term": {"stone_type": stone_type}})
#     if carat:
#         filters.append({
#             "range": {
#                 "stone_carat_weight": {
#                     "gte": carat - tolerance,
#                     "lte": carat + tolerance
#                 }
#             }
#         })
#     if gold_karat:
#         filters.append({"term": {"gold_karat": gold_karat}})

#     knn_query = {
#         "field": "embedding",
#         "query_vector": embedding,
#         "k": k,
#         "num_candidates": 50,
#         "filter": {
#             "bool": {
#                 "must": filters
#             }
#         }
#     }

#     resp = es.search(
#         index=INDEX_NAME,
#         knn=knn_query,
#         size=k,
#         _source=["name","price","url","stone_type","stone_carat_weight","gold_karat","source"]
#     )

#     hits = resp["hits"]["hits"]

#     # safeguard: enforce carat filter in Python
#     if carat:
#         hits = [
#             h for h in hits
#             if h["_source"].get("stone_carat_weight") is not None
#             and abs(h["_source"]["stone_carat_weight"] - carat) <= tolerance
#         ]

#     return [
#         {
#             "name": h["_source"].get("name"),
#             "price": h["_source"].get("price"),
#             "url": h["_source"].get("url"),
#             "stone_type": h["_source"].get("stone_type"),
#             "stone_carat_weight": h["_source"].get("stone_carat_weight"),
#             "gold_karat": h["_source"].get("gold_karat"),
#             "source": h["_source"].get("source"),
#             "score": h["_score"],
#         }
#         for h in hits
#     ]

# # --------------------------
# # Streamlit UI
# # --------------------------
# st.title("GemGem ↔ Competitor Pricing — Robust Normalizer Demo")
# uploaded = st.file_uploader("Upload GemGem CSV", type=["csv"])
# if not uploaded:
#     st.info("Upload your poc_gemgem.csv (it should have columns like name, price, listing_id, details).")
#     st.stop()

# df = pd.read_csv(uploaded)
# st.write(f"Loaded {len(df)} rows")

# sel_method = st.radio("Select by", ("Name", "ID"))
# if sel_method == "Name":
#     product_list = df["name"].astype(str).tolist()
# else:
#     product_list = df["listing_id"].astype(str).tolist()

# selected = st.selectbox("Product", product_list)
# row = df[df[sel_method.lower()].astype(str) == str(selected)].iloc[0]

# name = row.get("name", "")
# price = row.get("price", None)
# details_raw = row.get("details", None)

# normalized = normalize_gemgem_details(details_raw, name, price)
# st.subheader("Normalized Product (robust)")
# st.json(normalized)

# tolerance = st.slider("Carat tolerance (±)", 0.1, 2.0, 0.5, 0.1)
# st.write(f"Carat tolerance: ±{tolerance:.2f}")

# text = build_text_from_normalized(normalized)
# st.write("Canonical text for embedding:")
# st.write(text)

# embedding = model.encode(text, normalize_embeddings=True).tolist()

# try:
#     results = search_similar(es, embedding, normalized, tolerance, k=10)
# except Exception as e:
#     st.error(f"Elasticsearch query failed: {e}")
#     st.stop()

# if not results:
#     st.warning("No similar competitor products found.")
# else:
#     results_df = pd.DataFrame(results)
#     st.subheader("Top matches (from competitor_offers)")
#     st.dataframe(results_df[["name", "price", "url", "stone_type", "stone_carat_weight", "gold_karat","source"]])

#     prices = results_df["price"].dropna().astype(float).tolist()
#     avg_price = sum(prices) / len(prices) if prices else None
#     st.metric("Average competitor price", f"${avg_price:,.2f}" if avg_price else "N/A")
#     st.metric("GemGem price", f"${safe_float(price):,.2f}" if safe_float(price) else "N/A")

#     import matplotlib.pyplot as plt
#     labels = ["GemGem", "Competitor Avg"]
#     vals = [safe_float(price) or 0, avg_price or 0]
#     fig, ax = plt.subplots()
#     ax.bar(labels, vals)
#     ax.set_ylabel("Price ($)")
#     st.pyplot(fig)

# with st.expander("Raw details (for debugging)"):
#     st.write("details_raw:", details_raw)
#     st.write("parsed attempt:", try_parse_details_json(details_raw))

# gemgem/main.py
import os
import pandas as pd
import json
import re
import ast
import math
import streamlit as st
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch
from typing import Any, Dict, Optional
from dotenv import load_dotenv

# --------------------------
# Config via .env
# --------------------------
load_dotenv()

MODEL_NAME = os.getenv("MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
INDEX_NAME = os.getenv("INDEX_NAME", "competitor_offers")
ES_HOST = os.getenv("ES_HOST", "https://localhost:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASSWORD")

if not ES_PASS:
    raise RuntimeError("Elasticsearch password not set. Please add ES_PASS to .env")

# --------------------------
# Caching resources
# --------------------------
@st.cache_resource
def load_model():
    return SentenceTransformer(MODEL_NAME)

@st.cache_resource
def connect_es():
    return Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASS),
        verify_certs=False,   # ignore SSL verification
        ssl_show_warn=False   # suppress SSL warnings
    )

model = load_model()
es = connect_es()

# --------------------------
# Utility parsing helpers
# --------------------------
def safe_float(val):
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f):
            return None
        return f
    except Exception:
        return None

def parse_fraction_or_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return None
    s = s.replace("–", "-").replace("—", "-").replace(" ", "")
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(ctw|ct|carat)?", s, flags=re.IGNORECASE)
    if m and m.group(1):
        try:
            return float(m.group(1))
        except:
            pass
    if "-" in s or " " in s:
        sep = "-" if "-" in s else " "
        parts = s.split(sep)
        try:
            whole = float(parts[0])
            frac = parts[1]
            if "/" in frac:
                num, den = frac.split("/")
                return whole + float(num) / float(den)
            return whole + float(frac)
        except Exception:
            pass
    if "/" in s:
        try:
            num, den = s.split("/")
            return float(num) / float(den)
        except:
            pass
    num = re.sub(r"[^0-9.]", "", s)
    return safe_float(num)

def normalize_gold_karat(value: Any) -> Optional[int]:
    if value is None:
        return None
    s = str(value)
    m = re.search(r"(\d{1,2})\s*[Kk]", s)
    if m:
        try:
            return int(m.group(1))
        except:
            pass
    digits = re.sub(r"[^0-9]", "", s)
    if digits:
        try:
            return int(digits)
        except:
            pass
    return None

def extract_metal_type_and_color(s: str):
    if not s:
        return None, None
    s_low = s.lower()
    metal_type = None
    metal_color = None
    if "gold" in s_low:
        metal_type = "Gold"
    elif "silver" in s_low or "sterling" in s_low:
        metal_type = "Silver"
    elif "platinum" in s_low:
        metal_type = "Platinum"
    if "white" in s_low:
        metal_color = "White"
    elif "yellow" in s_low:
        metal_color = "Yellow"
    elif "rose" in s_low or "rose gold" in s_low:
        metal_color = "Rose"
    return metal_type, metal_color

def detect_stone_type_from_text(s: str) -> Optional[str]:
    if not s:
        return None
    s_low = s.lower()
    for stone in ["diamond", "ruby", "sapphire", "emerald", "opal", "topaz", "amethyst"]:
        if stone in s_low:
            return stone.capitalize()
    return None

def detect_shape_from_text(s: str) -> Optional[str]:
    if not s:
        return None
    s_low = s.lower()
    for shp in ["round", "princess", "pear", "oval", "emerald", "marquise", "cushion", "asscher", "radiant"]:
        if shp in s_low:
            return shp.capitalize()
    return None

def try_parse_details_json(raw):
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    text = str(raw).strip()
    for attempt in (json.loads, ast.literal_eval):
        try:
            parsed = attempt(text)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    cleaned = text.replace('""', '"')
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except:
        pass
    d = {}
    for m in re.finditer(r'"?([^":]+)"?\s*:\s*"?([^",}]+)"?', text):
        k = m.group(1).strip()
        v = m.group(2).strip()
        d[k] = v
    return d

# --------------------------
# Robust gemgem normalizer
# --------------------------
def normalize_gemgem_details(details_raw: Any, name: str, price: Any, category: Optional[str] = None) -> Dict[str, Any]:
    parsed = try_parse_details_json(details_raw)
    stone_section = None
    metal_section = None
    specs_section = None
    stone_keys = ["Stone(s)", "Stones", "stone(s)", "stone", "Stone"]
    metal_keys = ["Metal(s)", "Metals", "metal(s)", "metal", "Metal"]
    specs_keys = ["Specifications", "Specs", "specifications", "specs"]

    if isinstance(parsed, dict):
        for k in parsed.keys():
            if any(sk.lower() == k.lower() for sk in stone_keys):
                stone_section = parsed[k]
            if any(mk.lower() == k.lower() for mk in metal_keys):
                metal_section = parsed[k]
            if any(pk.lower() == k.lower() for pk in specs_keys):
                specs_section = parsed[k]

    def g(dict_or_none, *candidates):
        if not dict_or_none:
            return None
        if isinstance(dict_or_none, dict):
            for cand in candidates:
                for key in dict_or_none.keys():
                    if key and cand.lower().replace(" ", "") == str(key).lower().replace(" ", ""):
                        return dict_or_none.get(key)
        return None

    stone_type = g(stone_section, "Stone Type", "stone type", "stone")
    stone_shape = g(stone_section, "Diamond Shape", "Shape", "diamond shape")
    stone_clarity = g(stone_section, "Clarity")
    stone_color = g(stone_section, "Color")
    carat_raw = g(stone_section, "Carat Weight", "Carat")

    metal_full = None
    if isinstance(metal_section, dict):
        metal_full = g(metal_section, "Metal", "Metal Type")
    elif isinstance(metal_section, str):
        metal_full = metal_section

    gold_karat = normalize_gold_karat(g(metal_section, "Metal") or metal_full)
    metal_type, metal_color = extract_metal_type_and_color(metal_full or "")

    size_val = g(specs_section, "Size", "Length")
    ring_size = None
    if size_val:
        s = str(size_val).lower()
        if "cm" in s or "mm" in s or "in" in s:
            ring_size = None
        else:
            ring_size = parse_fraction_or_float(s)

    if carat_raw is None:
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(ctw|ct|carat)?", name, flags=re.IGNORECASE)
        if m and m.group(1):
            try:
                carat_val = float(m.group(1))
            except:
                carat_val = None
        else:
            carat_val = None
    else:
        carat_val = parse_fraction_or_float(carat_raw)

    if not stone_type:
        stone_type = detect_stone_type_from_text(name)
    if not stone_shape:
        stone_shape = detect_shape_from_text(name)

    price_val = safe_float(price)

    normalized = {
        "name": name,
        "price": price_val,
        "stone_type": stone_type,
        "stone_shape": stone_shape,
        "stone_clarity": stone_clarity,
        "stone_color": stone_color,
        "stone_carat_weight": carat_val,
        "metal_type": metal_type,
        "metal_color": metal_color,
        "gold_karat": gold_karat,
        "ring_size": ring_size,
        "source": "GemGem",
        "category": category
    }
    return normalized

def build_text_from_normalized(n):
    parts = [
        n.get("name") or "",
        f"{n.get('stone_type') or ''} {n.get('stone_shape') or ''} Clarity {n.get('stone_clarity') or ''} Color {n.get('stone_color') or ''} {n.get('stone_carat_weight') or ''} Carat",
        f"Metal: {n.get('metal_type') or ''} {n.get('metal_color') or ''} {n.get('gold_karat') or ''}",
        f"Ring Size: {n.get('ring_size') or ''}",
        f"Source: {n.get('source') or ''}"
    ]
    return " | ".join([p for p in parts if p and str(p).strip()])

# -------------------------------
# Search with filters + safeguards
# -------------------------------
def search_similar(es, embedding, normalized, tolerance, k=10):
    stone_type = normalized.get("stone_type")
    carat = normalized.get("stone_carat_weight")
    gold_karat = normalized.get("gold_karat")
    category = normalized.get("category")

    filters = []
    if stone_type:
        filters.append({"term": {"stone_type": stone_type}})
    if carat:
        filters.append({
            "range": {
                "stone_carat_weight": {
                    "gte": carat - tolerance,
                    "lte": carat + tolerance
                }
            }
        })
    if gold_karat:
        filters.append({"term": {"gold_karat": gold_karat}})
    if category:
        filters.append({"term": {"category.keyword": category}})

    knn_query = {
        "field": "embedding",
        "query_vector": embedding,
        "k": k,
        "num_candidates": 50,
        "filter": {
            "bool": {
                "must": filters
            }
        }
    }

    resp = es.search(
        index=INDEX_NAME,
        knn=knn_query,
        size=k,
        _source=["name","price","url","stone_type","stone_carat_weight","gold_karat","category","source"]
    )

    hits = resp["hits"]["hits"]

    # Python safeguard: ensure carat and category strictly match
    hits = [
        h for h in hits
        if (carat is None or (h["_source"].get("stone_carat_weight") is not None and abs(h["_source"]["stone_carat_weight"] - carat) <= tolerance))
        and (category is None or h["_source"].get("category") == category)
    ]

    return [
        {
            "name": h["_source"].get("name"),
            "price": h["_source"].get("price"),
            "url": h["_source"].get("url"),
            "stone_type": h["_source"].get("stone_type"),
            "stone_carat_weight": h["_source"].get("stone_carat_weight"),
            "gold_karat": h["_source"].get("gold_karat"),
            "category": h["_source"].get("category"),
            "source": h["_source"].get("source"),
            "score": h["_score"],
        }
        for h in hits
    ]

# --------------------------
# Streamlit UI
# --------------------------
st.title("GemGem ↔ Competitor Pricing — Robust Normalizer Demo")
uploaded = st.file_uploader("Upload GemGem CSV", type=["csv"])
if not uploaded:
    st.info("Upload your poc_gemgem.csv (it should have columns like name, price, listing_id, details, category).")
    st.stop()

df = pd.read_csv(uploaded)
st.write(f"Loaded {len(df)} rows")

sel_method = st.radio("Select by", ("Name", "ID"))
if sel_method == "Name":
    product_list = df["name"].astype(str).tolist()
else:
    product_list = df["listing_id"].astype(str).tolist()

selected = st.selectbox("Product", product_list)
row = df[df[sel_method.lower()].astype(str) == str(selected)].iloc[0]

name = row.get("name", "")
price = row.get("price", None)
details_raw = row.get("details", None)
category = row.get("category", None)

normalized = normalize_gemgem_details(details_raw, name, price, category)
st.subheader("Normalized Product (robust)")
st.json(normalized)

tolerance = st.slider("Carat tolerance (±)", 0.1, 2.0, 0.5, 0.1)
st.write(f"Carat tolerance: ±{tolerance:.2f}")

text = build_text_from_normalized(normalized)
st.write("Canonical text for embedding:")
st.write(text)

embedding = model.encode(text, normalize_embeddings=True).tolist()

try:
    results = search_similar(es, embedding, normalized, tolerance, k=10)
except Exception as e:
    st.error(f"Elasticsearch query failed: {e}")
    st.stop()

if not results:
    st.warning("No similar competitor products found.")
else:
    results_df = pd.DataFrame(results)
    st.subheader("Top matches (from competitor_offers)")
    st.dataframe(results_df[["name", "price", "url", "stone_type", "stone_carat_weight", "gold_karat","category","source"]])

    prices = results_df["price"].dropna().astype(float).tolist()
    avg_price = sum(prices) / len(prices) if prices else None
    st.metric("Average competitor price", f"${avg_price:,.2f}" if avg_price else "N/A")
    st.metric("GemGem price", f"${safe_float(price):,.2f}" if safe_float(price) else "N/A")

    import matplotlib.pyplot as plt
    labels = ["GemGem", "Competitor Avg"]
    vals = [safe_float(price) or 0, avg_price or 0]
    fig, ax = plt.subplots()
    ax.bar(labels, vals)
    ax.set_ylabel("Price ($)")
    st.pyplot(fig)

with st.expander("Raw details (for debugging)"):
    st.write("details_raw:", details_raw)
    st.write("parsed attempt:", try_parse_details_json(details_raw))

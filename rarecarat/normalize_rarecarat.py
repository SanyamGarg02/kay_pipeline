import csv
import json
import re

INPUT_CSV = "rarecarat/product_details.csv"
OUTPUT_CSV = "rarecarat/poc_rarecarat_normalized.csv"

# Normalized columns
NORMALIZED_FIELDS = [
    "stone_type",
    "stone_shape",
    "stone_clarity",
    "stone_color",
    "stone_carat_weight",
    "metal_type",
    "metal_color",
    "gold_karat",
    "ring_size"
]

def parse_metal_info(details):
    """
    Parse gold_carat and metal_type from details JSON.
    """
    gold_karat = details.get("gold_carat", "")
    metal_type = details.get("metal_type", "")
    metal_color = ""  # RareCarat does not separate color
    return gold_karat, metal_color, metal_type

def extract_attributes(details_json):
    """
    Parse the 'details' JSON and extract normalized attributes.
    """
    attrs = {field: "" for field in NORMALIZED_FIELDS}
    try:
        details = json.loads(details_json) if details_json else {}
    except json.JSONDecodeError:
        return attrs

    # All products are diamond
    attrs["stone_type"] = "Diamond"

    # Fill metal info
    gold_karat, metal_color, metal_type = parse_metal_info(details)
    attrs["gold_karat"] = gold_karat
    attrs["metal_type"] = metal_type
    attrs["metal_color"] = metal_color

    # Mapping from normalized fields to possible RareCarat keys
    mapping = {
        "stone_shape": ["Shape", "Stone Shape"],
        "stone_clarity": ["Clarity", "Diamond Clarity"],
        "stone_color": ["Color", "Diamond Color"],
        "stone_carat_weight": ["Carat", "Total Carat Weight (min)"],
        "ring_size": ["Sizes Available", "Ring Size"]
    }

    for norm_field, keys in mapping.items():
        for key in keys:
            if key in details:
                attrs[norm_field] = details[key]
                break

    return attrs

def infer_category(row):
    name = (row.get("name") or "").lower()
    url = (row.get("url") or "").lower()
    text = f"{name} {url}"

    # Order matters
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

if __name__ == "__main__":
    with open(INPUT_CSV, newline="", encoding="utf-8") as infile, \
         open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)
        fieldnames = ["name", "price", "url"] + NORMALIZED_FIELDS + ["category", "source"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            extracted = extract_attributes(row.get("details", ""))
            out_row = {
                "name": row.get("name", ""),
                "price": row.get("price", ""),
                "url": row.get("url", ""),
                **extracted,
                "category": infer_category(row),
                "source": "rarecarat"
            }
            writer.writerow(out_row)

    print(f"✅ Normalized CSV saved as {OUTPUT_CSV}")

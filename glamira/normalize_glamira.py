import csv
import json
import re

INPUT_CSV = "glamira/poc_glamira_final1.csv"
OUTPUT_CSV = "glamira/poc_glamira_normalized1.csv"


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

def parse_metal_info(value):
    """
    Parse a string like '18K White Gold' into gold_karat, metal_color, metal_type.
    """
    gold_karat, metal_color, metal_type = "", "", ""
    
    if not value:
        return gold_karat, metal_color, metal_type

    # Extract karat (e.g. 18K)
    karat_match = re.search(r"\d{1,2}K", value, re.IGNORECASE)
    if karat_match:
        gold_karat = karat_match.group(0)

    # Known metal types
    metals = ["Gold", "Platinum", "Silver", "Palladium"]
    for m in metals:
        if m.lower() in value.lower():
            metal_type = m
            break

    # Remove karat + metal type, the rest is color
    cleaned = value
    if gold_karat:
        cleaned = cleaned.replace(gold_karat, "")
    if metal_type:
        cleaned = re.sub(metal_type, "", cleaned, flags=re.IGNORECASE)
    
    metal_color = cleaned.strip()
    return gold_karat, metal_color, metal_type


def extract_attributes(details_json):
    """
    Parse the 'details' JSON and extract normalized attributes.
    Prioritizes Center Stone values over Accent Stone.
    """
    attrs = {field: "" for field in NORMALIZED_FIELDS}
    try:
        details = json.loads(details_json) if details_json else {}
    except json.JSONDecodeError:
        return attrs

    # Separate Center Stone vs others
    center_stone = {}
    other_sections = {}

    for section, items in details.items():
        if isinstance(items, dict):
            if "center stone" in section.lower():
                center_stone.update(items)
            else:
                other_sections.update(items)

    # Mapping from normalized fields to possible keys
    mapping = {
        "stone_type": ["Center Stone", "Stone", "Accent Stone"],
        "stone_shape": ["Shape[?]", "Center Stone Shape", "Stone Shape", "Shape"],
        "stone_clarity": ["Stone Clarity[?]", "Center Stone Clarity", "Clarity"],
        "stone_color": ["Color[?]", "Stone Color", "Center Stone Color", "Color"],
        "stone_carat_weight": [
            "Total Stone Carat[?]", "Center Stone Carat", "Stone Carat", "Carat", "Weight", "Total Stone Carat"
        ],
        "metal_type": ["Metal", "Metal Type"],  # will also be derived
        "metal_color": ["Metal Color", "Metal Colour"],  # will also be derived
        "gold_karat": ["Gold Karat", "Gold Karat Value"],  # will also be derived
        "ring_size": ["Ring Size", "Size"]
    }

    # Fill attributes, prioritizing Center Stone
    for norm_field, possible_keys in mapping.items():
        # Check Center Stone first
        for key in possible_keys:
            if key in center_stone:
                attrs[norm_field] = center_stone[key]
                break
        # If still empty, check other sections
        if not attrs[norm_field]:
            for key in possible_keys:
                if key in other_sections:
                    attrs[norm_field] = other_sections[key]
                    break

    # Special handling: "Color / Metal[?]" contains combined info
    if "Color / Metal[?]" in details.get("General", {}):
        gold_karat, metal_color, metal_type = parse_metal_info(details["General"]["Color / Metal[?]"])
        if gold_karat:
            attrs["gold_karat"] = gold_karat
        if metal_color:
            attrs["metal_color"] = metal_color
        if metal_type:
            attrs["metal_type"] = metal_type

    return attrs


if __name__ == "__main__":
    with open(INPUT_CSV, newline="", encoding="utf-8") as infile, \
         open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)
        fieldnames = ["name", "price", "url"] + NORMALIZED_FIELDS + ["source"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            extracted = extract_attributes(row.get("details", ""))
            out_row = {
                "name": row.get("name", ""),
                "price": row.get("price", ""),
                "url": row.get("url", ""),
                **extracted,
                "source": "Glamira"
            }
            writer.writerow(out_row)

    print(f"✅ Normalized CSV saved as {OUTPUT_CSV}")

import csv
import json

INPUT_CSV = "poc_kay_final.csv"
OUTPUT_CSV = "poc_kay_normalized.csv"

# Define normalized columns
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

def extract_attributes(details_json):
    """
    Parse the 'details' JSON and extract normalized attributes.
    Leave blank ("") if not found.
    """
    attrs = {field: "" for field in NORMALIZED_FIELDS}
    try:
        details = json.loads(details_json) if details_json else {}
    except json.JSONDecodeError:
        return attrs

    # Flatten all sections into key:value pairs
    flat = {}
    for section, items in details.items():
        flat.update(items)

    # Mapping
    mapping = {
        "stone_type": ["Stone Type"],
        "stone_shape": ["Stone Shape"],
        "stone_clarity": ["Clarity", "Stone 2 Diamond Clarity", "Stone 3 Diamond Clarity"],
        "stone_color": ["Color", "Stone Color", "Stone 2 Diamond Color", "Stone 3 Diamond Color"],
        "stone_carat_weight": ["Total Weight (CT. T.W.)", "Center Stone T.W."],
        "metal_type": ["Metal Type"],
        "metal_color": ["Metal Color"],
        "gold_karat": ["Gold Karat"],
        "ring_size": ["Standard Ring Size"]
    }

    for norm_field, possible_keys in mapping.items():
        for key in possible_keys:
            if key in flat:
                attrs[norm_field] = flat[key]
                break

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
                "source": "Kay Jewellers"
            }
            writer.writerow(out_row)

    print(f"✅ Normalized CSV saved as {OUTPUT_CSV}")

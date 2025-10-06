import subprocess
import sys
import argparse
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

def run_step(cmd, step_name):
    print(f"\n🚀 Running step: {step_name}")
    try:
        subprocess.run(cmd, check=True)
        print(f"✅ {step_name} completed successfully")
    except subprocess.CalledProcessError as e:
        print(f"❌ {step_name} failed with error code {e.returncode}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="End-to-end RareCarat competitor data pipeline")
    
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Products per category (default=200 if not provided)"
    )
    parser.add_argument(
        "--index",
        type=str,
        default="competitor_offers_test",    #updated for actual index
        help="Elasticsearch index to upload to"
    )
    parser.add_argument(
        "--password",
        type=str,
        default=None,
        help="Elasticsearch password for 'elastic' user (optional, fallback to .env)"
    )
    parser.add_argument(
        "--start-from",
        type=str,
        choices=["urls", "details", "normalize", "embeddings"],
        default="urls",
        help="Step to start pipeline from (default: urls)"
    )
    args = parser.parse_args()

    # Use password from .env if not provided
    es_password = args.password or os.getenv("ES_PASSWORD")
    if not es_password:
        print("❌ Elasticsearch password not provided. Set it via --password or in .env")
        sys.exit(1)

    # Step 1: Scrape URLs
    if args.start_from == "urls":
        cmd = [sys.executable, "rarecarat/urls_scraping.py"]
        if args.limit is not None:
            cmd += ["--limit", str(args.limit)]
        run_step(cmd, "Scraping RareCarat product URLs")

    # Step 2: Fetch details
    if args.start_from in ["urls", "details"]:
        run_step([sys.executable, "rarecarat/details_from_urls.py"], "Fetching RareCarat product details")

    # Step 3: Normalize dataset
    if args.start_from in ["urls", "details", "normalize"]:
        run_step([sys.executable, "rarecarat/normalize_rarecarat.py"], "Normalizing RareCarat dataset")

    # Step 4: Prepare embeddings + upload to ES
    if args.start_from in ["urls", "details", "normalize", "embeddings"]:
        run_step(
            [
                sys.executable, "rarecarat/prepare_embeddings_rarecarat.py",
                "--input", "rarecarat/poc_rarecarat_normalized.csv",
                "--output", f"rarecarat/{args.index}_rarecarat.ndjson",
                "--index", args.index,
                "--password", es_password
            ],
            "Preparing RareCarat embeddings + uploading to Elasticsearch"
        )

    print("\n🎉 RareCarat pipeline completed successfully!")

if __name__ == "__main__":
    main()

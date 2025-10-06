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
    parser = argparse.ArgumentParser(description="End-to-end 1stdibs data pipeline")

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Products per category (default=200 if not provided)"
    )
    parser.add_argument(
        "--index",
        type=str,
        default="competitor_offers_test",     #change this to index name u want to insert into
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
        choices=["urls", "details", "embeddings"],
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
        cmd = [sys.executable, "1stdibs/urls_scraping.py"]
        if args.limit is not None:
            cmd += ["--limit", str(args.limit)]
        run_step(cmd, "Scraping product URLs")

    # Step 2: Fetch details + normalize
    if args.start_from in ["urls", "details"]:
        run_step([sys.executable, "1stdibs/normalized_details_urls.py"], "Fetching details + normalizing dataset")

    # Step 3: Prepare embeddings + upload to ES
    if args.start_from in ["urls", "details", "embeddings"]:
        run_step(
            [
                sys.executable, "1stdibs/prepare_embeddings.py",
                "--input", "1stdibs/poc_1stdibs_normalized.csv",
                "--output", f"1stdibs/{args.index}_1stdibs.ndjson",
                "--index", args.index,
                "--password", es_password
            ],
            "Preparing 1stdibs embeddings + uploading to Elasticsearch"
        )

    print("\n🎉 1stdibs pipeline completed successfully!")

if __name__ == "__main__":
    main()

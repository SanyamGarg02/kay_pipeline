import subprocess
import sys
import argparse

def run_step(cmd, step_name):
    print(f"\n🚀 Running step: {step_name}")
    try:
        subprocess.run(cmd, check=True)
        print(f"✅ {step_name} completed successfully")
    except subprocess.CalledProcessError as e:
        print(f"❌ {step_name} failed with error code {e.returncode}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="End-to-end competitor data pipeline")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Products per category (default=200 if not provided)"
    )
    parser.add_argument(
        "--index",
        type=str,
        default="competitor_offers_test",
        help="Elasticsearch index to upload to"
    )
    parser.add_argument(
        "--password",
        type=str,
        required=True,
        help="Elasticsearch password for 'elastic' user"
    )
    parser.add_argument(
        "--start-from",
        type=str,
        choices=["urls", "details", "normalize", "embeddings"],
        default="urls",
        help="Step to start pipeline from (default: urls)"
    )
    args = parser.parse_args()

    # Step 1: Scrape URLs
    if args.start_from == "urls":
        cmd = [sys.executable, "urls_scraping.py"]
        if args.limit is not None:   # only pass if provided
            cmd += ["--limit", str(args.limit)]
        run_step(cmd, "Scraping product URLs")

    # Step 2: Fetch details
    if args.start_from in ["urls", "details"]:
        run_step([sys.executable, "details_from_urls.py"], "Fetching product details")

    # Step 3: Normalize dataset
    if args.start_from in ["urls", "details", "normalize"]:
        run_step([sys.executable, "normalize_dataset.py"], "Normalizing product dataset")

    # Step 4: Prepare embeddings + upload to ES
    if args.start_from in ["urls", "details", "normalize", "embeddings"]:
        run_step(
            [
                sys.executable, "prepare_embeddings.py",
                "--input", "poc_kay_normalized.csv",
                "--output", f"{args.index}.ndjson",
                "--index", args.index,
                "--password", args.password
            ],
            "Preparing embeddings + uploading to Elasticsearch"
        )

    print("\n🎉 Pipeline completed successfully!")

if __name__ == "__main__":
    main()

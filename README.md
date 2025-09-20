# 🪙 Competitor Data Pipeline

An automated pipeline to scrape competitor product data from [Kay.com](https://www.kay.com), normalize it, generate embeddings, and upload everything to **Elasticsearch** for search and analysis.

---

## 📂 Project Structure
Pipeline_testing/
│── details_from_urls.py # Scrape product details from URLs
│── normalize_dataset.py # Clean + normalize dataset
│── prepare_embeddings.py # Generate embeddings + upload to ES
│── run_pipeline.py # End-to-end pipeline runner (CLI)
│── urls_scraping.py # Scrape product URLs by category


---

## ⚙️ Requirements

### Install Python Packages
Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate    # Mac/Linux
.venv\Scripts\activate       # Windows

pip install -r requirements.txt

#---------------------------------

🚀 Run Elasticsearch

Before running the pipeline, Elasticsearch must be running locally.

Download Elasticsearch.

Extract and go to the Elasticsearch folder

Start Elasticsearch:

bin/elasticsearch

By default it runs at:
📍 http://localhost:9200

⚠️ You’ll need the password for the elastic user (set during setup).


#---------------------------------

▶️ Run the Pipeline

The entire pipeline can be run with one command:

python3 run_pipeline.py --password "your_es_password"    ##runs entire pipeline with 200 prodcuts per categpry ir 1200+products

Arguments

--limit → Number of products per category to scrape (default = 200 for full run, set lower for testing).

--index → Elasticsearch index name (e.g., competitor_offers or competitor_offers_test).

--password → Elasticsearch elastic user password.

--start-from → (optional) Choose which step to start from:

urls (default) → Scrape URLs → details → normalize → embeddings

details → Start from product details scraping

normalize → Start from normalization step

embeddings → Only embeddings + upload

#---------------------------------

📂 Outputs

products_list.csv → product URLs from scraping

poc_kay_final.csv → raw scraped product details

poc_kay_normalized.csv → normalized dataset

<index>.ndjson → embeddings uploaded to Elasticsearch
process FETCH_PAGE {

    maxForks 5
    publishDir "${params.resultsDir}/accession", mode: 'copy'

    input:
    val page_url

    output:
    file "page_*.csv"

    script:
    """
#!/usr/bin/env python3
import csv
import requests
import time
import sys
import random
import os
import json
import hashlib
from pathlib import Path
from urllib.parse import urlparse, parse_qsl, urlencode

url = "$page_url"
page_number = url.split("page=")[-1].split("&")[0] if "page=" in url else "1"
output_file = f"page_{page_number}.csv"

# Cache settings
CACHE_DIR = Path("${params.cacheDir}")
CACHE_DIR.mkdir(exist_ok=True)
SAMPLE_CACHE_DIR = CACHE_DIR / "samples"
SAMPLE_CACHE_DIR.mkdir(exist_ok=True)
MAX_AGE = 7 * 24 * 3600  # 7 days

MAX_RETRIES = 20
BASE_DELAY = 10
TIMEOUT = 120

def make_cache_key(url):
    parsed = urlparse(url)
    params = dict(parse_qsl(parsed.query))
    params.pop("page", None)  # don't include page number in key
    query_str = urlencode(sorted(params.items()))
    return hashlib.md5(query_str.encode("utf-8")).hexdigest()

def load_from_cache(cache_key, page):
    cache_file = CACHE_DIR / f"{cache_key}_page_{page}.json"
    if cache_file.exists() and time.time() - cache_file.stat().st_mtime < MAX_AGE:
        sys.stderr.write(f"[CACHE HIT] {cache_file}\\n")
        with open(cache_file) as f:
            return json.load(f)
    return None

def save_to_cache(cache_key, page, data):
    cache_file = CACHE_DIR / f"{cache_key}_page_{page}.json"
    with open(cache_file, "w") as f:
        json.dump(data, f)

def retry_get(url, retries=MAX_RETRIES, base_delay=BASE_DELAY, timeout=TIMEOUT):
    for i in range(retries):
        try:
            sys.stderr.write(f"Attempt {i+1}/{retries} for URL: {url}\\n")
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200:
                return response
            if 400 <= response.status_code < 500 and response.status_code != 429:
                sys.stderr.write(f"Client error {response.status_code} — not retrying.\\n")
                break
            sys.stderr.write(f"Server error {response.status_code} — will retry.\\n")
        except requests.RequestException as e:
            sys.stderr.write(f"Network error: {e}\\n")
        sleep_time = base_delay * (2 ** i) + random.uniform(0, base_delay)
        sys.stderr.write(f"Sleeping {sleep_time:.1f}s before retry...\\n")
        time.sleep(sleep_time)
    raise Exception(f"ERROR: Failed to fetch URL after {retries} retries: {url}")

def fetch_sample_biome(sample_id):
    cache_file = SAMPLE_CACHE_DIR / f"{sample_id}.json"
    # 1. Check local cache
    if cache_file.exists() and time.time() - cache_file.stat().st_mtime < MAX_AGE:
        sys.stderr.write(f"[CACHE HIT] {cache_file}\\n")
        with open(cache_file) as f:
            cached = json.load(f)
        return cached.get("biome", "N/A")
    # 2. Fetch from API
    sample_url = f"https://www.ebi.ac.uk/metagenomics/api/v1/samples/{sample_id}"
    try:
        response = retry_get(sample_url)
        data = response.json()
        biome_id = (
            data.get('data', {})
                .get('relationships', {})
                .get('biome', {})
                .get('data', {})
                .get('id', 'N/A')
        )
        # 3. Save to cache
        with open(cache_file, "w") as f:
            json.dump({"biome": biome_id}, f)
        return biome_id
    except Exception as e:
        sys.stderr.write(f"Failed to fetch biome for sample {sample_id}: {e}\\n")
        return "N/A"

def fetch_and_write(url, output_file):
    cache_key = make_cache_key(url)
    page = page_number
    data = load_from_cache(cache_key, page)
    if data is None:
        try:
            response = retry_get(url)
            data = response.json().get("data", [])
        except ValueError as e:
            sys.stderr.write(f"Failed to parse JSON from {url}: {e}\\n")
            sys.stderr.write(f"Response content: {response.text[:500]}...\\n")
            sys.exit(1)
        except Exception as e:
            sys.stderr.write(f"Failed to fetch data from {url}: {e}\\n")
            sys.exit(1)
        if isinstance(data, dict):
            data = [data]
        save_to_cache(cache_key, page, data)

    with open(output_file, mode='w', newline='') as csvfile:
        fieldnames = ["accession", "version", "experiment", "biome"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for entry in data:
            if isinstance(entry, dict):
                attributes = entry.get("attributes", {})
                relationships = entry.get("relationships", {})
                sample_id = relationships.get("sample", {}).get("data", {}).get("id")
                row = {
                    "accession": attributes.get("accession", "N/A"),
                    "version": attributes.get("pipeline-version", "N/A"),
                    "experiment": attributes.get("experiment-type", "N/A"),
                    "biome": fetch_sample_biome(sample_id) if sample_id else "N/A"
                }
                writer.writerow(row)
            else:
                sys.stderr.write(f"Skipping invalid entry: {entry}\\n")

fetch_and_write(url, output_file)
    """
}

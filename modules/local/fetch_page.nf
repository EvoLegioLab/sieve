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

url = "$page_url"
page_number = url.split("page=")[-1].split("&")[0] if "page=" in url else "1"
output_file = f"page_{page_number}.csv"

MAX_RETRIES = 20
BASE_DELAY = 10
TIMEOUT = 120

def retry_get(url, retries=MAX_RETRIES, base_delay=BASE_DELAY, timeout=TIMEOUT):
    for i in range(retries):
        try:
            sys.stderr.write(f"Attempt {i+1}/{retries} for URL: {url}\\n")
            response = requests.get(url, timeout=timeout)

            if response.status_code == 200:
                return response

            # Fail fast for client errors (except 429 Too Many Requests)
            if 400 <= response.status_code < 500 and response.status_code != 429:
                sys.stderr.write(f"Client error {response.status_code} — not retrying.\\n")
                break

            sys.stderr.write(f"Server error {response.status_code} — will retry.\\n")

        except requests.RequestException as e:
            sys.stderr.write(f"Network error: {e}\\n")

        # Exponential backoff with jitter
        sleep_time = base_delay * (2 ** i) + random.uniform(0, base_delay)
        sys.stderr.write(f"Sleeping {sleep_time:.1f}s before retry...\\n")
        time.sleep(sleep_time)

    raise Exception(f"ERROR: Failed to fetch URL after {retries} retries: {url}")

def fetch_sample_biome(sample_id):
    sample_url = f"https://www.ebi.ac.uk/metagenomics/api/v1/samples/{sample_id}"
    try:
        response = retry_get(sample_url)
        data = response.json()
        return data.get('data', {}).get('relationships', {}).get('biome', {}).get('data', {}).get('id', 'N/A')
    except Exception as e:
        sys.stderr.write(f"Failed to fetch biome for sample {sample_id}: {e}\\n")
        return "N/A"

def fetch_and_write(url, output_file):
    try:
        response = retry_get(url)
        try:
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

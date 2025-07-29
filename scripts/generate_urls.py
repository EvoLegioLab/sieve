#!/usr/bin/env python3
import os
import requests

API_BASE = "https://www.ebi.ac.uk/metagenomics/api/v1/analyses"

# Initialize params with mandatory page_size
params = {
    "page_size": os.getenv("PAGE_SIZE", "100")
}

# Add optional parameters only if not empty
if os.getenv("BIOME_NAME"):
    params["biome_name"] = os.getenv("BIOME_NAME")
if os.getenv("LINEAGE"):
    params["lineage"] = os.getenv("LINEAGE")
if os.getenv("EXPERIMENT_TYPE"):
    params["experiment_type"] = os.getenv("EXPERIMENT_TYPE")
if os.getenv("STUDY_ACCESSION"):
    params["study_accession"] = os.getenv("STUDY_ACCESSION")
if os.getenv("SAMPLE_ACCESSION"):
    params["sample_accession"] = os.getenv("SAMPLE_ACCESSION")
if os.getenv("INSTRUMENT_PLATFORM"):
    params["instrument_platform"] = os.getenv("INSTRUMENT_PLATFORM")
if os.getenv("INSTRUMENT_MODEL"):
    params["instrument_model"] = os.getenv("INSTRUMENT_MODEL")
if os.getenv("PIPELINE_VERSION"):
    params["pipeline_version"] = os.getenv("PIPELINE_VERSION")

# Fetch first page to get total number of pages
response = requests.get(API_BASE, params=params)
response.raise_for_status()
data = response.json()

total_pages = data.get("meta", {}).get("pagination", {}).get("pages", 1)

with open("page_urls.txt", "w") as f:
    for page in range(1, total_pages + 1):
        page_params = params.copy()
        page_params["page"] = page
        url = f"{API_BASE}?" + "&".join(f"{k}={v}" for k, v in page_params.items())
        f.write(url + "\n")

print(f"Generated URLs for {total_pages} pages.")

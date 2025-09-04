#!/usr/bin/env python3
import os
import requests
import sys
import json

ANALYSES_BASE = "https://www.ebi.ac.uk/metagenomics/api/v1/analyses"
STUDIES_BASE = "https://www.ebi.ac.uk/metagenomics/api/v1/studies"

params = {
    "page_size": os.getenv("PAGE_SIZE", "100")
}

# Optional filters
if os.getenv("BIOME_NAME"):
    params["biome_name"] = os.getenv("BIOME_NAME")
if os.getenv("LINEAGE"):
    params["lineage"] = os.getenv("LINEAGE")
if os.getenv("EXPERIMENT_TYPE"):
    params["experiment_type"] = os.getenv("EXPERIMENT_TYPE")
if os.getenv("SAMPLE_ACCESSION"):
    params["sample_accession"] = os.getenv("SAMPLE_ACCESSION")
if os.getenv("INSTRUMENT_PLATFORM"):
    params["instrument_platform"] = os.getenv("INSTRUMENT_PLATFORM")
if os.getenv("INSTRUMENT_MODEL"):
    params["instrument_model"] = os.getenv("INSTRUMENT_MODEL")
if os.getenv("PIPELINE_VERSION"):
    params["pipeline_version"] = os.getenv("PIPELINE_VERSION")

study_accession = os.getenv("STUDY_ACCESSION")

with open("page_urls.txt", "w") as f:
    if study_accession:
        sys.stderr.write(f"Expanding study {study_accession} into individual analyses\n")
        API_BASE = f"{STUDIES_BASE}/{study_accession}/analyses"

        page = 1
        while True:
            page_params = params.copy()
            page_params["page"] = page
            sys.stderr.write(f"\nFetching page {page} from {API_BASE} with params {page_params}\n")

            response = requests.get(API_BASE, params=page_params)
            sys.stderr.write(f"Request URL: {response.url}\n")
            sys.stderr.write(f"Response status: {response.status_code}\n")

            try:
                data = response.json()
            except Exception as e:
                sys.stderr.write(f"Failed to parse JSON: {e}\n")
                sys.stderr.write(f"Response text: {response.text[:500]}\n")
                break

            analyses = data.get("data", [])
            # Normalize single analysis to a list
            if isinstance(analyses, dict):
                analyses = [analyses]

            sys.stderr.write(f"Found {len(analyses)} analyses on page {page}\n")

            if not analyses:
                sys.stderr.write("No analyses found, stopping.\n")
                break

            for ana in analyses:
                attrs = ana.get("attributes", {})
                accession = attrs.get("accession")
                exp_type = attrs.get("experiment-type", "")
                if accession:
                    if exp_type in ["assembly", "metagenomic"]:  # Only include these types
                        url = f"{ANALYSES_BASE}/{accession}"
                        f.write(url + "\n")
                        sys.stderr.write(f"Added analysis URL: {url}\n")
                    else:
                        sys.stderr.write(f"Skipping analysis {accession} with type: {exp_type}\n")
                else:
                    sys.stderr.write(f"Warning: analysis entry missing accession field: {json.dumps(ana)[:200]}\n")

            # Pagination
            pagination = data.get("meta", {}).get("pagination", {})
            total_pages = pagination.get("pages", 1)
            sys.stderr.write(f"Pagination info: page {page} of {total_pages}\n")

            if page >= total_pages:
                sys.stderr.write("Reached last page.\n")
                break
            page += 1

    else:
        # Global analyses search
        API_BASE = ANALYSES_BASE
        sys.stderr.write("Generating paged URLs from global analyses endpoint\n")

        response = requests.get(API_BASE, params=params)
        sys.stderr.write(f"Request URL: {response.url}\n")
        response.raise_for_status()
        data = response.json()

        total_pages = data.get("meta", {}).get("pagination", {}).get("pages", 1)
        sys.stderr.write(f"Found {total_pages} total pages\n")

        for page in range(1, total_pages + 1):
            page_params = params.copy()
            page_params["page"] = page
            url = f"{API_BASE}?" + "&".join(f"{k}={v}" for k, v in page_params.items())
            f.write(url + "\n")
            sys.stderr.write(f"Generated page URL: {url}\n")

print("Finished generating URLs.")

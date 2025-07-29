process FETCH_PAGE {

    maxForks 10  // limit to 5 concurrent instances

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
    import sys
    import time
    import json

    url = "$page_url"

    # Derive output file name from URL
    page_number = url.split("page=")[-1].split("&")[0] if "page=" in url else "1"
    output_file = f"page_{page_number}.csv"

    def fetch_sample_biome(sample_id):
        try:
            sample_url = f"https://www.ebi.ac.uk/metagenomics/api/v1/samples/{sample_id}"
            response = requests.get(sample_url)
            if response.status_code == 200:
                data = response.json()
                return data.get('data', {}).get('relationships', {}).get('biome', {}).get('data', {}).get('id', 'N/A')
            else:
                return 'N/A'
        except Exception:
            return 'N/A'

    def fetch_and_write(url, output_file):
        with open(output_file, mode='w', newline='') as csvfile:
            fieldnames = ["accession", "version", "experiment", "biome"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            response = requests.get(url)
            response.raise_for_status()
            data = response.json().get("data", [])

            for entry in data:
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

    fetch_and_write(url, output_file)
    """
}

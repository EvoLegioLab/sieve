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

    url = "$page_url"
    page_number = url.split("page=")[-1].split("&")[0] if "page=" in url else "1"
    output_file = f"page_{page_number}.csv"

    def retry_get(url, retries=20, delay=10):
        for i in range(retries):
            try:
                sys.stderr.write(f"Attempt {i+1}/{retries} for URL: {url}\\n")
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    return response
                else:
                    sys.stderr.write(f"Non-200 response ({response.status_code}) for URL: {url}\\n")
            except requests.RequestException as e:
                sys.stderr.write(f"Retrying ({i+1}/{retries}) after error: {e}\\n")
            time.sleep(delay * (i + 1))  # Exponential backoff
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

    def fetch_and_write(url, output_file, retries=20, delay=10):
        try:
            response = retry_get(url)
            try:
                data = response.json().get("data", [])
            except ValueError as e:
                sys.stderr.write(f"Failed to parse JSON from {url}: {e}\\n")
                sys.stderr.write(f"Response content: {response.text}\\n")
                sys.exit(1)
        except Exception as e:
            sys.stderr.write(f"Failed to fetch data from {url}: {e}\\n")
            sys.exit(1)

        # Normalize single-object responses into a list
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

process ACCESSION_TEST {

    publishDir "$params.resultsDir/accession", mode:'copy'

    input:
    val file_name
    val biome_name
    val lineage
    val experiment_type
    val study_accession
    val sample_accession
    val instrument_platform
    val instrument_model
    val pipeline_version
    val page_size

    output:
    file "${file_name}"

    script:
    """  
    #!/usr/bin/env python
    # -*- coding: utf-8 -*-

    import argparse
    import csv
    import sys
    import logging
    import requests
    import urllib.request
    import json
    import time
    from urllib.parse import urlencode
    from jsonapi_client import Filter, Session

    sys.setrecursionlimit(40000)

    API_BASE = "https://www.ebi.ac.uk/metagenomics/api/v1"

    file_name = "$file_name"
    biome_name = "$biome_name"
    lineage = "$lineage"
    experiment_type = "$experiment_type"
    study_accession = "$study_accession"
    sample_accession = "$sample_accession"
    instrument_platform = "$instrument_platform"
    instrument_model = "$instrument_model"
    pipeline_version = "$pipeline_version"
    page_size = "$page_size"

    def construct_url(base_url, params):
        query_string = urlencode(params)
        return f"{base_url}/analyses?{query_string}"

    def get_page(url, max_retries=5):
        next_url = url
        retries = 0
        while next_url:
            try:
                with urllib.request.urlopen(next_url) as page:
                    response = json.loads(page.read().decode())
                    data = response['data']
                    yield data
                    next_url = response['links'].get('next')
                    retries = 0  # reset retries on success
                    print(next_url)
            except urllib.error.HTTPError as e:
                if e.code == 500:
                    retries += 1
                    if retries > max_retries:
                        print(f"Max retries reached for {next_url}. Exiting.")
                        break
                    print(f"Server error 500 on {next_url}, retrying {retries}/{max_retries} after delay...")
                    time.sleep(10)
                    continue
                else:
                    raise

    def write_data_to_csv(url, output_csv):
        with open(output_csv, mode='w', newline='') as csvfile:
            # Define the field names (columns) for the CSV file
            fieldnames = ["accession", "version", "experiment", "biome"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write the header row to the CSV
            writer.writeheader()
            
            # Fetch pages using get_page
            for page_data in get_page(url):
                time.sleep(1)
                for analyses in page_data:
                    try:
                        # Extract data from the correct location in the JSON structure
                        attributes = analyses.get('attributes', {})
                        relationships = analyses.get('relationships', {})
                        sample_id = relationships.get("sample", {}).get("data", {}).get("id", None)
                        
                        # Construct the row data
                        row = {
                            "accession": attributes.get("accession", "N/A"),
                            "version": attributes.get("pipeline-version", "N/A"),
                            "experiment": attributes.get("experiment-type", "N/A"),
                            "biome": "N/A"  # Default value
                        }

                        # If there is a sample ID, make an additional request to get the biome data
                        if sample_id:
                            sample_url = f"https://www.ebi.ac.uk/metagenomics/api/v1/samples/{sample_id}"
                            sample_response = requests.get(sample_url)
                            if sample_response.status_code == 200:
                                sample_data = sample_response.json()
                                biome_id = sample_data.get('data', {}).get('relationships', {}).get('biome', {}).get('data', {}).get('id', 'N/A')
                                row['biome'] = biome_id
                            else:
                                print(f"Failed to fetch sample data for ID {sample_id}, Status Code: {sample_response.status_code}")
                        
                        # Print the constructed row for debugging
                        print(f"Constructed Row: {row}")

                        # Write the row to the CSV file
                        writer.writerow(row)
                        print(f"Done for {row['accession']}")
                    except Exception as e:
                        print(f"Error processing analysis: {analyses}, Error: {e}")


    with open(file_name, "w") as csvfile:
        # CSV initialization
        fieldnames = ["accession", "version", "experiment", "biome"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        with Session(API_BASE) as session:
            params = {
                'ordering': 'accession',
                'page_size': page_size,
            }
            if experiment_type != "null":
                params['experiment_type'] = experiment_type
            if study_accession != "null":
                params['study_accession'] = study_accession
            if pipeline_version != "null":
                params['pipeline_version'] = pipeline_version
            if instrument_platform != "null":
                params['instrument_platform'] = instrument_platform
            if instrument_model != "null":
                params['instrument_model'] = instrument_model
            if sample_accession != "null":
                params['sample_accession'] = sample_accession
            if lineage != "null":
                params['lineage'] = lineage
            if biome_name != "null":
                params['biome_name'] = biome_name

            # Construct URL
            url = construct_url(API_BASE, params)
            print(f"Constructed URL: {url}")


            # Log the constructed URL for debugging purposes
            logging.info(f"Constructed URL: {url}")

            # Write data to CSV using the constructed URL
            output_csv = file_name  # Assuming `file_name` contains the desired output file name
            write_data_to_csv(url, output_csv)

            print("Finished writing data to CSV.")

    """
}




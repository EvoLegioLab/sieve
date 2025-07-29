process ACCESSION_CONCURRENT_TEST {

    publishDir "$params.resultsDir/accession", mode: 'copy'

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
    #!/usr/bin/env python3

    import csv
    import sys
    import time
    import json
    import requests
    from urllib.parse import urlencode
    from concurrent.futures import ThreadPoolExecutor, as_completed

    sys.setrecursionlimit(40000)

    API_BASE = "https://www.ebi.ac.uk/metagenomics/api/v1"
    MAX_WORKERS = 10
    MAX_RETRIES = 5
    TIMEOUT = 10

    file_name = "${file_name}"
    biome_name = "${biome_name}"
    lineage = "${lineage}"
    experiment_type = "${experiment_type}"
    study_accession = "${study_accession}"
    sample_accession = "${sample_accession}"
    instrument_platform = "${instrument_platform}"
    instrument_model = "${instrument_model}"
    pipeline_version = "${pipeline_version}"
    page_size = "${page_size}"

    def construct_url(base_url, params):
        query_string = urlencode(params)
        return f"{base_url}/analyses?{query_string}"

    def get_page(url):
        next_url = url
        retries = 0
        while next_url:
            try:
                response = requests.get(next_url, timeout=TIMEOUT)
                if response.status_code == 200:
                    data = response.json()
                    yield data['data']
                    next_url = data.get('links', {}).get('next')
                    retries = 0
                elif response.status_code == 500:
                    if retries < MAX_RETRIES:
                        retries += 1
                        print(f"Retrying {retries}/{MAX_RETRIES} after server error...")
                        time.sleep(5)
                    else:
                        print("Max retries exceeded.")
                        break
                else:
                    print(f"Failed to get page: {response.status_code}")
                    break
            except Exception as e:
                print(f"Exception while fetching page: {e}")
                break

    def fetch_biome(sample_id):
        sample_url = f"{API_BASE}/samples/{sample_id}"
        try:
            response = requests.get(sample_url, timeout=TIMEOUT)
            if response.status_code == 200:
                sample_data = response.json()
                biome_id = sample_data.get('data', {}).get('relationships', {}).get('biome', {}).get('data', {}).get('id', 'N/A')
                return sample_id, biome_id
            else:
                return sample_id, 'N/A'
        except Exception:
            return sample_id, 'N/A'

    def write_data_to_csv(url, output_csv):
        all_rows = []
        sample_ids_to_fetch = set()

        for page_data in get_page(url):
            for analysis in page_data:
                attributes = analysis.get('attributes', {})
                relationships = analysis.get('relationships', {})
                sample_id = relationships.get("sample", {}).get("data", {}).get("id", None)

                row = {
                    "accession": attributes.get("accession", "N/A"),
                    "version": attributes.get("pipeline-version", "N/A"),
                    "experiment": attributes.get("experiment-type", "N/A"),
                    "biome": "N/A",
                    "sample_id": sample_id
                }

                if sample_id:
                    sample_ids_to_fetch.add(sample_id)

                all_rows.append(row)

        sample_biome_map = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_biome, sid): sid for sid in sample_ids_to_fetch}
            for future in as_completed(futures):
                sid, biome = future.result()
                sample_biome_map[sid] = biome

        with open(output_csv, mode='w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["accession", "version", "experiment", "biome"])
            writer.writeheader()
            for row in all_rows:
                sample_id = row.pop("sample_id", None)
                if sample_id:
                    row["biome"] = sample_biome_map.get(sample_id, "N/A")
                writer.writerow(row)

    if __name__ == "__main__":
        params = {
            'ordering': 'accession',
            'page_size': page_size
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

        query_url = construct_url(API_BASE, params)
        print(f"Query URL: {query_url}")
        write_data_to_csv(query_url, file_name)
        print(f"Finished writing to {file_name}")
    """
}

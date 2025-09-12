process DOWNLOAD {
    maxForks 10  // limit to 10 concurrent instances

    tag "$accession"

    input:
    tuple val(accession), val(experiment), val(biome)
    val outputDir

    output:
    tuple val(accession), val(experiment), val(biome), path('*.fasta.gz')

    script:
    """
    #!/usr/bin/env python
    # -*- coding: utf-8 -*-

    import os
    import requests
    from jsonapi_client import Session

    API_BASE = "https://www.ebi.ac.uk/metagenomics/api/v1"

    accession = '$accession'
    experiment = '$experiment'
    lineage = "$biome"
    outputDir = "$outputDir"

    def download_and_concatenate(session, accession, experiment):
        if experiment == "metagenomic":
            label = 'Processed nucleotide reads'
        elif experiment == "assembly":
            label = 'Processed contigs'
        else:
            label = None

        print(f"DEBUG: Starting download for accession '{accession}' with experiment type '{experiment}'")
        print(f"DEBUG: Looking for files with label '{label}' and format 'FASTA' or 'FASTQ'")

        file_list = []
        try:
            api_url = f"{API_BASE}/analyses/{accession}/downloads"
            print(f"DEBUG: Accessing API endpoint: {api_url}")

            downloads = list(session.iterate(f"analyses/{accession}/downloads"))
            print(f"DEBUG: Found {len(downloads)} download entries for accession {accession}")
        except Exception as e:
            print(f"ERROR: Could not retrieve downloads list for accession {accession}: {e}")
            downloads = []

        for download in downloads:
            print(f"DEBUG: Found download entry - label: '{download.description.label}', format: '{download.file_format.name}', alias: '{download.alias}'")
            if label and download.description.label == label and download.file_format.name in ('FASTA', 'FASTQ'):
                local_file = download.alias  # keep original name & extension
                try:
                    print(f"DEBUG: Preparing to download '{download.alias}' for accession '{accession}'")
                    print(f"DEBUG: Download URL: {download.links.self.url}")

                    headers = {"User-Agent": "Mozilla/5.0 (compatible; SIEVE/1.0)"}
                    response = requests.get(download.links.self.url, headers=headers, stream=True)

                    if response.status_code == 200:
                        with open(local_file, "wb") as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        if os.path.exists(local_file):
                            print(f"DEBUG: Successfully downloaded '{local_file}'")
                            file_list.append(local_file)
                        else:
                            print(f"WARNING: File '{local_file}' does not exist after download attempt")
                    else:
                        print(f"ERROR: Failed to download '{local_file}'. HTTP {response.status_code}")
                except Exception as e:
                    print(f"ERROR: Failed to download file '{download.alias}' for accession '{accession}': {e}")

        if not file_list:
            print(f"WARNING: No matching files downloaded for accession '{accession}'. Nothing to concatenate.")
        else:
            try:
                output_file = f"{accession}.fasta.gz"
                print(f"DEBUG: Concatenating {len(file_list)} files into '{output_file}'")
                cat_command = "cat {} > {}".format(" ".join(file_list), output_file)
                ret = os.system(cat_command)
                if ret != 0:
                    print(f"WARNING: Concatenation command exited with code {ret}")

                # Clean up individual files
                for f in file_list:
                    try:
                        os.remove(f)
                        print(f"DEBUG: Removed temporary file '{f}'")
                    except Exception as e:
                        print(f"WARNING: Could not remove temporary file '{f}': {e}")

                print(f"DEBUG: Concatenation and cleanup completed for accession '{accession}'.")
            except Exception as e:
                print(f"ERROR: Error during concatenation for accession '{accession}': {e}")

    if __name__ == "__main__":
        print("DEBUG: Opening session to EBI Metagenomics API")
        try:
            with Session(API_BASE) as session:
                download_and_concatenate(session, accession, experiment)
        except Exception as e:
            print(f"ERROR: Could not open session or complete downloads for accession '{accession}': {e}")

        print(f"DEBUG: Download process finished for accession '{accession}'")
    """
}

process GENERATE_PAGE_URLS {

    publishDir "$params.resultsDir", mode: 'copy'

    input:
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
    path "page_urls.txt"

    script:
script:
"""
export BIOME_NAME="${biome_name == null || biome_name == 'null' ? '' : biome_name}"
export LINEAGE="${lineage == null || lineage == 'null' ? '' : lineage}"
export EXPERIMENT_TYPE="${experiment_type == null || experiment_type == 'null' ? '' : experiment_type}"
export STUDY_ACCESSION="${study_accession == null || study_accession == 'null' ? '' : study_accession}"
export SAMPLE_ACCESSION="${sample_accession == null || sample_accession == 'null' ? '' : sample_accession}"
export INSTRUMENT_PLATFORM="${instrument_platform == null || instrument_platform == 'null' ? '' : instrument_platform}"
export INSTRUMENT_MODEL="${instrument_model == null || instrument_model == 'null' ? '' : instrument_model}"
export PIPELINE_VERSION="${pipeline_version == null || pipeline_version == 'null' ? '' : pipeline_version}"
export PAGE_SIZE="${page_size == null || page_size == 'null' ? '100' : page_size}"

python3 ../../../scripts/generate_urls.py

echo "Files in current dir:"
ls -l

echo "Content of page_urls.txt:"
cat page_urls.txt
"""

}

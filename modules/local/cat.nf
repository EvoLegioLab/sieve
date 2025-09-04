process CAT { 
    publishDir "$params.resultsDir/contigs/classification/", pattern: "*_summary.txt"
    tag "$accession"

    input:
    tuple val(accession), val(experiment), val(biome), path(contig), path(reads)
    val cat_db
    val cat_taxonomy

    output: 
    tuple val(accession), path('*.alignment.diamond'), path('*.faa'), emit: classification
    path ('*_summary.txt'), emit: file_contig_classification

    script:
    """
    # Prepare contigs: decompress only if gzip magic number detected
    if [[ \$(head -c 2 "$contig" | od -An -tx1 | tr -d ' ') == "1f8b" ]]; then
        gunzip -c "$contig" > ${accession}.fasta
    else
        cp "$contig" ${accession}.fasta
    fi

    # Run classification of contigs
    CAT contigs -c ${accession}.fasta -d "$cat_db" -t "$cat_taxonomy" -n "$task.cpus" -o "$accession"

    # Add official names
    CAT add_names --only_official -i "$accession".contig2classification.txt -t "$cat_taxonomy" -o "$accession"classification_official_names.txt

    # Add non-official names
    CAT add_names -i "$accession".contig2classification.txt -t "$cat_taxonomy" -o "$accession"classification_names.txt

    # Summarize results
    CAT summarise -c ${accession}.fasta -i "$accession"classification_official_names.txt -o "$accession"classification_summary.txt
    """
}

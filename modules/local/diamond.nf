process DIAMOND {
    tag "$accession"

    input:
      tuple val(accession), val(experiment), val(biome), path(reads)
      file ref_db
      val cpus
      val min_align_reads

    output:
      tuple val(accession), val(experiment), val(biome), path(reads), path("${accession}.tsv"), optional: true

    script:
    // Determine reads and reference database names
    def reads_name = (reads instanceof List) ? reads.collect { it.getName() }.join(' ') : reads.getName()
    def ref_db_name = ref_db.getName()

    // Build DIAMOND command
    def diamond_cmd = [
        'diamond blastx',
        "-q ${reads_name}",
        "--db ${ref_db_name}",
        "--out ${accession}.tsv",
        "--threads ${cpus}",
        "--outfmt 6"
    ]

    if (params.user_diamond_options && params.user_diamond_options.trim()) {
        diamond_cmd << params.user_diamond_options.trim()
    } else {
        diamond_cmd << "--unal 0 --id 85 -e 1e-6"
    }

    // Prepare output folder for missing genes
    def missing_dir = "${params.resultsDir}/diamond_missing"
    """
    mkdir -p ${missing_dir}

    echo "=========================="
    echo "Running DIAMOND for: ${accession}"
    echo "Reads: ${reads_name}"
    echo "Ref DB: ${ref_db_name}"
    echo "CPUs: ${cpus}"
    echo "Min aligned reads: ${min_align_reads}"
    echo "ALL_GENES Mode: ${params.all_genes}"
    echo "DIAMOND CMD: ${diamond_cmd.join(' ')}"
    echo "=========================="

    # Run DIAMOND
    ${diamond_cmd.join(' ')}

    if [ -f "${accession}.tsv" ]; then
        total_hits=\$(wc -l < "${accession}.tsv")
        echo "Total DIAMOND hits: \$total_hits"

        # Fail if zero hits
        if [ "\$total_hits" -eq 0 ]; then
            echo "No alignments found — removing ${accession}.tsv"
            rm "${accession}.tsv"
            exit 0
        fi

        if [ "${params.all_genes}" = "true" ]; then
            echo "Running per-gene filtering..."

            # Extract expected gene list from FASTA filenames
            expected_genes=\$(ls ${params.genes}/*.fasta | sed 's#.*/##' | sed 's/.fasta//')
            echo "\$expected_genes" > expected_genes.list

            # Count hits per gene from DIAMOND output (column 2)
            awk '{counts[\$2]++} END {for (g in counts) print g, counts[g]}' "${accession}.tsv" > gene_counts.tsv
            echo "Gene counts (first 20 lines):"
            head -n 20 gene_counts.tsv || true

            # Report all genes, even if count is 0
            > "${missing_dir}/${accession}_gene_counts.tsv"
            echo -e "gene_name\tobserved_count\trequired_min_reads" >> "${missing_dir}/${accession}_gene_counts.tsv"
            for gene in \$expected_genes; do
                count=\$(awk -v g="\$gene" '\$1==g {print \$2}' gene_counts.tsv)
                count=\${count:-0}  # set to 0 if gene not found
                echo -e "\$gene\t\$count\t${min_align_reads}" >> "${missing_dir}/${accession}_gene_counts.tsv"
            done

            # Determine if sample passes (any gene < min_align_reads)
            fail_count=\$(awk -v min=${min_align_reads} 'NR>1 && \$2 < min {count++} END {print count}' "${missing_dir}/${accession}_gene_counts.tsv")
            if [ "\$fail_count" -gt 0 ]; then
                echo "Sample failed per-gene threshold. See ${missing_dir}/${accession}_gene_counts.tsv"
                rm "${accession}.tsv"
            else
                echo "All genes meet ≥ ${min_align_reads} reads. Sample PASSED."
            fi

        else
            # Default: check total hits
            if [ "\$total_hits" -le "${min_align_reads}" ]; then
                echo "Too few total alignments — excluding sample"
                rm "${accession}.tsv"
            else
                echo "Sample passed total hit threshold"
            fi
        fi
    else
        echo "ERROR: ${accession}.tsv not found after DIAMOND!"
    fi

    echo "=========================="
    """
}

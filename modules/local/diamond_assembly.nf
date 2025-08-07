process DIAMOND_ASSEMBLY {
    tag "$accession"

    input:
    tuple val(accession), val(experiment), val(biome), path(contigs), val(quality), val(bin_name)
    file ref_db
    val cpus


    output:
      tuple val(accession), val(experiment), val(biome), path(contigs), path("*.tsv"), optional: true

    script:
    def contig_file_name = contigs.getName()
    def ref_db_name = ref_db.getName()

    def diamond_cmd = [
        'diamond blastx',
        "-q ${contig_file_name}",
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

    """
    ${diamond_cmd.join(' ')}

    if [ -f "${accession}.tsv" ]; then
        hit_count=\$(wc -l < "${accession}.tsv")
        echo "Hits for ${accession}: \$hit_count"

        if [ "\$hit_count" -le "${params.diamond_min_align_assembly}" ]; then
            echo "Too few hits (<=${params.diamond_min_align_assembly}), removing ${accession}.tsv"
            rm "${accession}.tsv"
        fi
    fi
    """
}

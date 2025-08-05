process DIAMOND {
    tag "$accession"

    input:
      tuple val(accession), val(experiment), val(biome), path(reads)
      file ref_db
      val cpus
      val min_align_reads

    output:
    tuple val(accession), val(experiment), val(biome), path(reads), path("*.tsv"), optional: true

script:
def reads_name = (reads instanceof List)
    ? reads.collect { it.getName() }.join(' ')
    : reads.getName()

def ref_db_name  = ref_db.getName()


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

"""
   # Run DIAMOND with format 6 output
    ${diamond_cmd.join(' ')}

    # If too few reads in total then omit that sample
    # For each hit in the tsv output, record which gene had a hit. Then check in a user provided csv file whether there are more or equally many hits than wanted for all genes. If there are, pass along. If not, delete. awk
    if [ -f "${accession}.tsv" ]; then
      # Count lines (hits)
      hit_count=\$(wc -l < "${accession}.tsv")
      echo "Hits for ${accession}: \$hit_count"

      if [ "\$hit_count" -le "${min_align_reads}" ]; then
        echo "Too few alignments (<=${min_align_reads}), removing ${accession}.tsv"
        rm "${accession}.tsv"
      fi
    fi

"""
}

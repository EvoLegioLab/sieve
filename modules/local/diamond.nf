process DIAMOND {
    tag "$accession"

    input:
      tuple val(accession), val(experiment), val(biome), path(reads)
      file ref_db
      val cpus
      val min_align_reads

    output:
    tuple val(accession), val(experiment), val(biome), path(reads), path("*.daa"), optional: true

script:
def reads_name = (reads instanceof List)
    ? reads.collect { it.getName() }.join(' ')
    : reads.getName()

def ref_db_name  = ref_db.getName()


def diamond_cmd = [
    'diamond blastx',
    "-q ${reads_name}",
    "--db ${ref_db_name}",
    "--out ${accession}.daa",
    "--threads ${cpus}"
]

if (params.user_diamond_options && params.user_diamond_options.trim()) {
    diamond_cmd << params.user_diamond_options.trim()
} else {
    diamond_cmd << "-f 100 --unal 0 --id 85 -e 1e-6"
}

"""
# Run DIAMOND
${diamond_cmd.join(' ')}

# Filter by minimum alignment count
if [ -f "${accession}.daa" ]; then
    align_count=\$(diamond view --daa "${accession}.daa" | wc -l)
    if [ "\$align_count" -le "${min_align_reads}" ]; then
        echo "Fewer than ${min_align_reads} alignments — removing output."
        rm "${accession}.daa"
    fi
    """


}

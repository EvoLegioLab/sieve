process ADAPTERREMOVAL {
    tag "$accession"

    input:
    tuple val(single_end), val(accession), val(experiment), val(biome), path(reads)
    path(adapterlist)

    output:
    tuple val(accession), val(experiment), val(biome), path("*_trimSE.fasta.gz"), optional: true, emit: singles_truncated
    tuple val(accession), val(experiment), val(biome), path("*_trimPE.fasta.gz"), optional: true, emit: paired_truncated

    script:
    def args = task.ext.args ?: ''
    def list = adapterlist ? "--adapter-list ${adapterlist}" : ""

    if (single_end == true) {
        """
        #!/usr/bin/env bash
        set -euo pipefail

        AdapterRemoval \\
            --file1 $reads \\
            $args \\
            $list \\
            --basename $accession \\
            --threads $task.cpus \\
            --seed 42 \\
            --gzip

        zcat ${accession}.truncated.gz \\
          | awk 'NR%4==1 {print ">" substr(\$0,2)} NR%4==2 {print}' \\
          | gzip > ${accession}_trimSE.fasta.gz
        """
    } else {
        def read1 = reads[0]
        def read2 = reads[1]

        """
        #!/usr/bin/env bash
        set -euo pipefail

        AdapterRemoval \\
            --file1 ${read1} \\
            --file2 ${read2} \\
            $args \\
            $list \\
            --basename $accession \\
            --threads $task.cpus \\
            --collapse \\
            --seed 42 \\
            --gzip

        echo "Combining collapsed and collapsed.truncated outputs"
        files=()
        [[ -s "${accession}.collapsed.gz" ]] && files+=( "${accession}.collapsed.gz" )
        [[ -s "${accession}.collapsed.truncated.gz" ]] && files+=( "${accession}.collapsed.truncated.gz" )

        if [ \${#files[@]} -gt 0 ]; then
            for f in "\${files[@]}"; do
                zcat "\$f" \\
                  | awk 'NR%4==1 {print ">" substr(\$0,2)} NR%4==2 {print}'
            done | gzip > "${accession}_trimPE.fasta.gz"
        else
            echo "No collapsed reads found. Using trimmed pairs."
            for mate in 1 2; do
                if [[ -s "${accession}.pair\${mate}.truncated.gz" ]]; then
                    zcat "${accession}.pair\${mate}.truncated.gz" \\
                      | awk 'NR%4==1 {print ">" substr(\$0,2)} NR%4==2 {print}'
                fi
            done | gzip > "${accession}_trimPE.fasta.gz"
        fi
        """
    }
}

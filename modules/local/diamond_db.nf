process DIAMOND_DB {

    tag "${task.hash}"

    input:
    path fasta_files

    output:
    path "references.dmnd"

    script:
    """
    cat ${fasta_files.join(' ')} > references.fasta
    diamond makedb --in references.fasta --db references.dmnd
    """
}


process CONTIGS_COVERAGE {
  tag "$accession"

  cpus 8
  memory '32.GB'

  errorStrategy 'ignore'
  maxRetries 2

  input:
    tuple val(accession), val(experiment), val(biome), path(contig), path(reads)

  output:
    tuple val(accession), val(experiment), val(biome), path(contig),
          path("*_abundance.txt"), path("*_aln.sorted.*"), optional: true

  script:
  """
  set -euo pipefail

  # Re-index large contigs safely
  bwa index -a bwtsw "$contig" -p "${accession}_index"

  # Run alignment separately
  bwa mem -t ${task.cpus} "${accession}_index" "$reads" > "${accession}_aln.sam"

  # Then convert and sort
  samtools view -b -@ ${task.cpus} "${accession}_aln.sam" > "${accession}_aln.bam"
  samtools sort -@ ${task.cpus} "${accession}_aln.bam" > "${accession}_aln.sorted.bam"
  samtools index "${accession}_aln.sorted.bam"

  # Coverage and abundance
  pileup.sh in="${accession}_aln.sam" out="${accession}_cov.txt"
  awk '{print \$1\"\\t\"\$5}' "${accession}_cov.txt" | grep -v '^#' > "${accession}_abundance.txt"
  """
}

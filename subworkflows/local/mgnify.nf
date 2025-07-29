/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
// MODULE: Installed directly from nf-core/modules
//
<<<<<<< HEAD
include { ACCESSION                      } from '../../modules/local/accession'
include { TAXONOMY                       } from '../../modules/local/taxonomy'
include { DOWNLOAD                       } from '../../modules/local/download'
=======
include { ACCESSION_CONCURRENT_TEST } from '../../modules/local/accession_concurrent_test'
include { TAXONOMY }                 from '../../modules/local/taxonomy'
include { DOWNLOAD }                 from '../../modules/local/download'
include { GENERATE_PAGE_URLS }       from '../../modules/local/generate_page_urls'
include { FETCH_PAGE }               from '../../modules/local/fetch_page'
>>>>>>> master

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    EXECUTE SUBWORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
workflow MGNIFY {
    take:
<<<<<<< HEAD
    file_name // file name (set as default in nextflow.config file)

    main:
    ch_accession = ACCESSION (
        file_name, params.biome_name, params.lineage, params.experiment_type, params.study_accession, params.sample_accession, params.instrument_platform, params.instrument_model, params.pipeline_version, params.page_size
    )

    if (params.taxonomyphylum == "null" && params.taxonomyclass == "null" && params.taxonomyorder == "null" && params.taxonomyfamily == "null" && params.taxonomygenus == "null" && params.taxonomyspecies == "null"){
            ch_taxonomy = ch_accession
            | splitCsv(header: true)
            | map { row -> [row.accession, row.experiment, row.biome]}
    }
    else {
        ch_split_accession = ch_accession
            | splitCsv(header: true)
            | map { row -> [row.accession, row.version, row.experiment, row.biome]}
            
        TAXONOMY(params.taxonomyphylum, params.taxonomyclass, params.taxonomyorder, params.taxonomyfamily, params.taxonomygenus, params.taxonomyspecies, ch_split_accession)
        
        ch_taxonomy = TAXONOMY.out.tax_id
            | splitCsv(header: true)
            | map { row -> [row.accession, row.experiment, row.biome]}
    }


    ch_download = DOWNLOAD(ch_taxonomy, params.resultsDir)

    emit:
    ch_download                              // channel: [val(accession), val(experiment), val(lineage), path(reads)]

=======
    file_name

    main:
    // Generate URLs to fetch pages from
    page_urls_ch = GENERATE_PAGE_URLS(
        params.biome_name,
        params.lineage,
        params.experiment_type,
        params.study_accession,
        params.sample_accession,
        params.instrument_platform,
        params.instrument_model,
        params.pipeline_version,
        params.page_size
    )

    // Read all lines from the emitted file as individual URLs
    page_url_lines = page_urls_ch
        .map { file -> file.readLines() }
        .flatten()

    // Fetch pages concurrently; output is channel of individual CSV files (one per page)
    fetch_page_out = FETCH_PAGE(page_url_lines)

    // For each CSV file, split into rows and map to needed fields
    ch_accession = fetch_page_out
        .splitCsv(header: true)
        .map { row -> [row.accession, row.version, row.experiment, row.biome] }

    // If no taxonomy filters set, skip TAXONOMY step
    if (params.taxonomyphylum == "null" && params.taxonomyclass == "null" && params.taxonomyorder == "null" && params.taxonomyfamily == "null" && params.taxonomygenus == "null" && params.taxonomyspecies == "null") {
        ch_taxonomy = ch_accession
    } else {
        // Run taxonomy filtering on accession channel
        taxonomy_out = TAXONOMY(
            params.taxonomyphylum, params.taxonomyclass, params.taxonomyorder,
            params.taxonomyfamily, params.taxonomygenus, params.taxonomyspecies,
            ch_accession
        )

        // Extract relevant fields from taxonomy output
        ch_taxonomy = taxonomy_out.tax_id
            .splitCsv(header: true)
            .map { row -> [row.accession, row.experiment, row.biome] }
    }

    // Download step takes the taxonomy-filtered accessions
    ch_download = DOWNLOAD(ch_taxonomy, params.resultsDir)

    emit:
    ch_download
>>>>>>> master
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
// MODULE: Installed directly from nf-core/modules
//
include { ACCESSION_CONCURRENT_TEST } from '../../modules/local/accession_concurrent_test'
include { TAXONOMY }                 from '../../modules/local/taxonomy'
include { DOWNLOAD }                 from '../../modules/local/download'
include { GENERATE_PAGE_URLS }       from '../../modules/local/generate_page_urls'
include { FETCH_PAGE }               from '../../modules/local/fetch_page'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    EXECUTE SUBWORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
workflow MGNIFY {
    take:
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
        params.page_size,
	file('scripts/generate_urls.py')
    )

    // Read all lines from the emitted file as individual URLs
    page_url_lines = page_urls_ch
        .map { file -> file.readLines() }
        .flatten()

    // Fetch pages concurrently; output is channel of individual CSV files (one per page)
    fetch_page_out = FETCH_PAGE(page_url_lines)

    // For each CSV file, split into rows and map to needed fields (include version!)
    ch_accession = fetch_page_out
        .splitCsv(header: true)
        .map { row -> [row.accession, row.version, row.experiment, row.biome] }

    // If no taxonomy filters set, skip TAXONOMY step
    if (params.taxonomyphylum == "null" && params.taxonomyclass == "null" &&
        params.taxonomyorder == "null" && params.taxonomyfamily == "null" &&
        params.taxonomygenus == "null" && params.taxonomyspecies == "null") {

        // Drop version field for download
        ch_taxonomy = ch_accession
            .map { row -> [row[0], row[2], row[3]] } // accession, experiment, biome

    } else {
        // Run taxonomy filtering on full 4-tuple
        taxonomy_out = TAXONOMY(
            params.taxonomyphylum, params.taxonomyclass, params.taxonomyorder,
            params.taxonomyfamily, params.taxonomygenus, params.taxonomyspecies,
            ch_accession
        )

        // Extract relevant fields from taxonomy output and drop version
        ch_taxonomy = taxonomy_out.tax_id
            .splitCsv(header: true)
            .map { row -> [row.accession, row.experiment, row.biome] }
    }

    // Download step takes the taxonomy-filtered accessions
    ch_download = DOWNLOAD(ch_taxonomy, params.resultsDir)

    emit:
    ch_download
}

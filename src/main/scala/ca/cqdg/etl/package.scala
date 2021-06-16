package ca.cqdg

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.loader.Format.{CSV, JSON, PARQUET}
import bio.ferlab.datalake.spark3.loader.LoadType.OverWrite

package object etl {

  val clinical_data: String = "clinical-data"
  val ontology_input: String = "ontology-input"
  val clinical_data_etl_indexer: String = "clinical-data-etl-indexer"
  val tsv_with_headers = Map("sep" -> "\t", "header" -> "true")
  val tsv_without_headers = Map("sep" -> "\t", "header" -> "false")

  val etlConfiguration: Configuration = Configuration(
    storages = List(
      StorageConf(clinical_data, s"s3a://cqdg/${clinical_data}"),
      StorageConf(ontology_input, s"s3a://cqdg/${ontology_input}"),
      StorageConf(clinical_data_etl_indexer, s"s3a://cqdg/${clinical_data_etl_indexer}")),
    sources = List(
      //raw data
      DatasetConf("study_version_metadata", clinical_data, "/*/*/*/study_version_metadata.json", JSON, OverWrite),
      DatasetConf("biospecimen"           , clinical_data, "/*/*/*/biospecimen.tsv"            , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("diagnosis"             , clinical_data, "/*/*/*/diagnosis.tsv"              , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("donor"                 , clinical_data, "/*/*/*/donor.tsv"                  , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("exposure"              , clinical_data, "/*/*/*/exposure.tsv"               , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("family_history"        , clinical_data, "/*/*/*/family-history.tsv"         , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("family_relationship"   , clinical_data, "/*/*/*/family-relationship.tsv"    , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("file"                  , clinical_data, "/*/*/*/file.tsv"                   , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("follow_up"             , clinical_data, "/*/*/*/follow-up.tsv"              , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("phenotype"             , clinical_data, "/*/*/*/phenotype.tsv"              , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("sample_registration"   , clinical_data, "/*/*/*/sample_registration.tsv"    , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("study"                 , clinical_data, "/*/*/*/study.tsv"                  , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("treatment"             , clinical_data, "/*/*/*/treatment.tsv"              , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("data_access"           , clinical_data, "/*/*/*/data_access.tsv"            , CSV , OverWrite, readoptions = tsv_with_headers),

      DatasetConf("hpo"                   , ontology_input, "/hpo_terms.json.gz"               , JSON, OverWrite),
      DatasetConf("mondo"                 , ontology_input, "/mondo_terms.json.gz"             , JSON, OverWrite),

      //intermediate data
      DatasetConf("donor_diagnosis", clinical_data_etl_indexer, "/donor_diagnosis", PARQUET, OverWrite),
      DatasetConf("donor_families" , clinical_data_etl_indexer, "/donor_families" , PARQUET, OverWrite),


      //data to index
      DatasetConf("donors" , clinical_data_etl_indexer, "/donors" ,  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date")),
      DatasetConf("studies", clinical_data_etl_indexer, "/studies",  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date")),
      DatasetConf("files"  , clinical_data_etl_indexer, "/files"  ,  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date"))
    )
  )
}

package ca.cqdg.etl.processes

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.loader.Format.{CSV, JSON, PARQUET}
import bio.ferlab.datalake.spark3.loader.LoadType.OverWrite

class ProcessETLTestConfig(input: String, ontology: String, output: String) {

  val processETLConfig: Configuration = Configuration(
    storages = List(
      StorageConf("input", input),
      StorageConf("ontology", ontology),
      StorageConf("output", output)
    ),
    sources = List(
      //raw data
      DatasetConf("biospecimen"           , "input", "/biospecimen.tsv"            , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("diagnosis"             , "input", "/diagnosis.tsv"              , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("donor"                 , "input", "/donor.tsv"                  , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("exposure"              , "input", "/exposure.tsv"               , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("family_history"        , "input", "/family-history.tsv"         , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("family"                , "input", "/family.tsv"                 , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("file"                  , "input", "/file.tsv"                   , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("follow_up"             , "input", "/follow-up.tsv"              , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("phenotype"             , "input", "/phenotype.tsv"              , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("sample_registration"   , "input", "/sample_registration.tsv"    , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("study"                 , "input", "/study.tsv"                  , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("treatment"             , "input", "/treatment.tsv"              , CSV , OverWrite, readoptions = tsv_with_headers),

      DatasetConf("hpo"                   , "ontology", "/hpo_terms.json"               , JSON, OverWrite),
      DatasetConf("mondo"                 , "ontology", "/mondo_terms.json"             , JSON, OverWrite),
      DatasetConf("icd"                   , "ontology", "/icd_terms.json"               , JSON, OverWrite),
      DatasetConf("duo_code"              , "ontology", "/duo_code_terms.json"          , JSON, OverWrite),

      //data to index
      DatasetConf("donors" , "output", "/donors" ,  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date")),
      DatasetConf("studies", "output", "/studies",  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date")),
      DatasetConf("files"  , "output", "/files"  ,  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date"))
    )
  )

}

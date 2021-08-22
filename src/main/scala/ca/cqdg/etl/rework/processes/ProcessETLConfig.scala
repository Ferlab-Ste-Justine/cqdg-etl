package ca.cqdg.etl.rework.processes

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.loader.Format.{JSON, PARQUET}
import bio.ferlab.datalake.spark3.loader.LoadType.OverWrite

class ProcessETLConfig(input: String, ontology: String, output: String) {

  val processETLConfig: Configuration = Configuration(
    storages = List(
      StorageConf("input", input),
      StorageConf("ontology", ontology),
      StorageConf("output", output)
    ),
    sources = List(
      //raw data
      DatasetConf("biospecimen"           , "input", "/biospecimen"            , PARQUET , OverWrite),
      DatasetConf("diagnosis"             , "input", "/diagnosis"              , PARQUET , OverWrite),
      DatasetConf("donor"                 , "input", "/donor"                  , PARQUET , OverWrite),
      DatasetConf("exposure"              , "input", "/exposure"               , PARQUET , OverWrite),
      DatasetConf("family_history"        , "input", "/family-history"         , PARQUET , OverWrite),
      DatasetConf("family"                , "input", "/family"                 , PARQUET , OverWrite),
      DatasetConf("file"                  , "input", "/file"                   , PARQUET , OverWrite),
      DatasetConf("follow_up"             , "input", "/follow-up"              , PARQUET , OverWrite),
      DatasetConf("phenotype"             , "input", "/phenotype"              , PARQUET , OverWrite),
      DatasetConf("sample_registration"   , "input", "/sample_registration"    , PARQUET , OverWrite),
      DatasetConf("study"                 , "input", "/study"                  , PARQUET , OverWrite),
      DatasetConf("treatment"             , "input", "/treatment"              , PARQUET , OverWrite),

      DatasetConf("hpo"                   , "ontology", "/hpo_terms.json.gz"               , JSON, OverWrite),
      DatasetConf("mondo"                 , "ontology", "/mondo_terms.json.gz"             , JSON, OverWrite),
      DatasetConf("icd"                   , "ontology", "/icd_terms.json.gz"               , JSON, OverWrite),
      DatasetConf("duo_code"              , "ontology", "/duo_code_terms.json.gz"          , JSON, OverWrite),

      //data to index
      DatasetConf("donors" , "output", "/donors" ,  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date")),
      DatasetConf("studies", "output", "/studies",  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date")),
      DatasetConf("files"  , "output", "/files"  ,  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date"))
    )
  )

}

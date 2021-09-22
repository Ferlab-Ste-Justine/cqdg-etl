package ca.cqdg.etl.processes

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.loader.Format.{CSV, JSON, PARQUET}
import bio.ferlab.datalake.spark3.loader.LoadType.OverWrite

class PreProcessETLTestConfig(input: String, output: String) {

  val preProcessETLConfig: Configuration = Configuration(
    storages = List(
      StorageConf("input", input),
      StorageConf("output", output)
    ),
    sources = List(
      // raw data
      DatasetConf("study_version_metadata"  , "input", "/study_version_metadata.json" , JSON, OverWrite, readoptions = json_multiline),
      DatasetConf("biospecimen"             , "input", "/biospecimen.tsv"             , CSV, OverWrite, readoptions = tsv_with_headers),
      DatasetConf("diagnosis"               , "input", "/diagnosis.tsv"               , CSV, OverWrite, readoptions = tsv_with_headers),
      DatasetConf("donor"                   , "input", "/donor.tsv"                   , CSV, OverWrite, readoptions = tsv_with_headers),
      DatasetConf("exposure"                , "input", "/exposure.tsv"                , CSV, OverWrite, readoptions = tsv_with_headers),
      DatasetConf("family-history"          , "input", "/family-history.tsv"          , CSV, OverWrite, readoptions = tsv_with_headers),
      DatasetConf("family"                  , "input", "/family.tsv"                  , CSV, OverWrite, readoptions = tsv_with_headers),
      DatasetConf("file"                    , "input", "/file.tsv"                    , CSV, OverWrite, readoptions = tsv_with_headers),
      DatasetConf("follow-up"               , "input", "/follow-up.tsv"               , CSV, OverWrite, readoptions = tsv_with_headers),
      DatasetConf("phenotype"               , "input", "/phenotype.tsv"               , CSV, OverWrite, readoptions = tsv_with_headers),
      DatasetConf("sample_registration"     , "input", "/sample_registration.tsv"     , CSV, OverWrite, readoptions = tsv_with_headers),
      DatasetConf("study"                   , "input", "/study.tsv"                   , CSV, OverWrite, readoptions = tsv_with_headers),
      DatasetConf("treatment"               , "input", "/treatment.tsv"               , CSV, OverWrite, readoptions = tsv_with_headers),

      // data to index
      DatasetConf("biospecimen-with-ids"          , "output", "/biospecimen"        , CSV, OverWrite, writeoptions = tsv_with_headers),
      DatasetConf("diagnosis-with-ids"            , "output", "/diagnosis"          , CSV, OverWrite, writeoptions = tsv_with_headers),
      DatasetConf("donor-with-ids"                , "output", "/donor"              , CSV, OverWrite, writeoptions = tsv_with_headers),
      DatasetConf("exposure-with-ids"             , "output", "/exposure"           , CSV, OverWrite, writeoptions = tsv_with_headers),
      DatasetConf("family-history-with-ids"       , "output", "/family-history"     , CSV, OverWrite, writeoptions = tsv_with_headers),
      DatasetConf("family-with-ids"               , "output", "/family"             , CSV, OverWrite, writeoptions = tsv_with_headers),
      DatasetConf("file-with-ids"                 , "output", "/file"               , CSV, OverWrite, writeoptions = tsv_with_headers),
      DatasetConf("follow-up-with-ids"            , "output", "/follow-up"          , CSV, OverWrite, writeoptions = tsv_with_headers),
      DatasetConf("phenotype-with-ids"            , "output", "/phenotype"          , CSV, OverWrite, writeoptions = tsv_with_headers),
      DatasetConf("sample_registration-with-ids"  , "output", "/sample_registration", CSV, OverWrite, writeoptions = tsv_with_headers),
      DatasetConf("study-with-ids"                , "output", "/study"              , CSV, OverWrite, writeoptions = tsv_with_headers),
      DatasetConf("treatment-with-ids"            , "output", "/treatment"          , CSV, OverWrite, writeoptions = tsv_with_headers),
    )
  )
}

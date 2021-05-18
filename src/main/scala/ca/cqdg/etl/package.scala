package ca.cqdg

import bio.ferlab.datalake.spark3.config.{Configuration, SourceConf, StorageConf}
import bio.ferlab.datalake.spark3.loader.Format.{CSV, JSON}
import bio.ferlab.datalake.spark3.loader.LoadType.OverWrite

package object etl {

  val cqdg: String = "cqdg"
  val tsv_with_headers = Map("sep" -> "\t", "header" -> "true")
  val tsv_without_headers = Map("sep" -> "\t", "header" -> "false")

  implicit val etlConfiguration: Configuration = Configuration(
    storages = List(StorageConf("cgdg", "s3a://cqdg")),
    sources = List(
      //raw data
      SourceConf(cqdg, "/clinical-data/biospecimen.tsv"        , "", "biospecimen"        , CSV, OverWrite, readoptions = tsv_with_headers),
      SourceConf(cqdg, "/clinical-data/diagnosis.tsv"          , "", "diagnosis"          , CSV, OverWrite, readoptions = tsv_with_headers),
      SourceConf(cqdg, "/clinical-data/donor.tsv"              , "", "donor"              , CSV, OverWrite, readoptions = tsv_with_headers),
      SourceConf(cqdg, "/clinical-data/exposure.tsv"           , "", "exposure"           , CSV, OverWrite, readoptions = tsv_with_headers),
      SourceConf(cqdg, "/clinical-data/family-history.tsv"     , "", "family-history"     , CSV, OverWrite, readoptions = tsv_with_headers),
      SourceConf(cqdg, "/clinical-data/family-relationship.tsv", "", "family-relationship", CSV, OverWrite, readoptions = tsv_with_headers),
      SourceConf(cqdg, "/clinical-data/file.tsv"               , "", "file"               , CSV, OverWrite, readoptions = tsv_with_headers),
      SourceConf(cqdg, "/clinical-data/follow-up.tsv"          , "", "follow-up"          , CSV, OverWrite, readoptions = tsv_with_headers),
      SourceConf(cqdg, "/clinical-data/phenotype.tsv"          , "", "phenotype"          , CSV, OverWrite, readoptions = tsv_with_headers),
      SourceConf(cqdg, "/clinical-data/sample_registration.tsv", "", "sample_registration", CSV, OverWrite, readoptions = tsv_with_headers),
      SourceConf(cqdg, "/clinical-data/study.tsv"              , "", "study"              , CSV, OverWrite, readoptions = tsv_with_headers),
      SourceConf(cqdg, "/clinical-data/treatment.tsv"          , "", "treatment"          , CSV, OverWrite, readoptions = tsv_with_headers),

      //data to index
      SourceConf(cqdg, "/clinical-data-etl-indexer/donors" , "", "donors" , JSON, OverWrite),
      SourceConf(cqdg, "/clinical-data-etl-indexer/studies", "", "studies", JSON, OverWrite),
      SourceConf(cqdg, "/clinical-data-etl-indexer/files"  , "", "files"  , JSON, OverWrite)
    )
  )

}

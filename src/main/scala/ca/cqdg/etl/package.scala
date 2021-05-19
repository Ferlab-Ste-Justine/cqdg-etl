package ca.cqdg

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.loader.Format.{CSV, JSON, PARQUET}
import bio.ferlab.datalake.spark3.loader.LoadType.{Insert, OverWrite}
import org.apache.spark.sql.{DataFrame, SparkSession}

package object etl {

  val clinical_data: String = "clinical-data"
  val clinical_data_etl_indexer: String = "clinical-data-etl-indexer"
  val tsv_with_headers = Map("sep" -> "\t", "header" -> "true")
  val tsv_without_headers = Map("sep" -> "\t", "header" -> "false")

  val etlConfiguration: Configuration = Configuration(
    storages = List(
      StorageConf(clinical_data, "s3a://cqdg/clinical-data"),
      StorageConf(clinical_data_etl_indexer, "s3a://cqdg/clinical-data-etl-indexer")),
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

      //intermediate data
      DatasetConf("donor_families", clinical_data_etl_indexer, "/donor_families", PARQUET, OverWrite),


      //data to index
      DatasetConf("donors" ,clinical_data_etl_indexer, "/donors" ,  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date")),
      DatasetConf("studies",clinical_data_etl_indexer, "/studies",  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date")),
      DatasetConf("files"  ,clinical_data_etl_indexer, "/files"  ,  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date"))
    )
  )

  implicit class SparkOps(spark: SparkSession) {
    def readConf(sc: DatasetConf)(implicit conf: Configuration): DataFrame = {
      spark.read.format(sc.format.sparkFormat).options(sc.readoptions).load(sc.location)
    }
  }


  implicit class DataFrameOps(df: DataFrame) {
    def writeConf(sc: DatasetConf)(implicit conf: Configuration): DataFrame = {

      val dfw = sc.partitionby match {
        case Nil =>
          df.write
            .format(sc.format.sparkFormat)
            .option("path", sc.location)
        case _ =>
          df.write
            .partitionBy(sc.partitionby:_*)
            .format(sc.format.sparkFormat)
            .option("path", sc.location)
      }

      if (sc.table.nonEmpty) {
        dfw.mode("overwrite").saveAsTable(s"${sc.table.get.fullName}")
      } else {
        dfw.mode("overwrite").save()
      }

      df
    }
  }

}

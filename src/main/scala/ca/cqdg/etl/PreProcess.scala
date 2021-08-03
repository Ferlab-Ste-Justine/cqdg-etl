package ca.cqdg.etl

import ca.cqdg.etl.model.{NamedDataFrame, S3File}
import ca.cqdg.etl.utils.PreProcessingUtils.{loadSchemas, preProcess}
import ca.cqdg.etl.utils.S3Utils.writeSuccessIndicator
import ca.cqdg.etl.utils.{S3Utils, Schema}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

object PreProcess extends App{

  val preProcessConfig: Config = ConfigFactory.load.getObject("pre-process").toConfig
  val preProcessLogger = preProcessConfig.getString("logger")
  val preProcessInput = preProcessConfig.getString("input")
  val preProcessOutput = preProcessConfig.getString("output")
  val preProcessFormat = preProcessConfig.getString("format")

  val log = LoggerFactory.getLogger(preProcessLogger)
  Logger.getLogger(preProcessLogger).setLevel(Level.INFO)

  def run(): Unit = {
    log.info("Looking for un-processed S3 files ...")
    val filesPerFolder: Map[String, List[S3File]] = S3Utils.loadFileEntries(s3Bucket, preProcessInput, s3Client)
    log.info("Retrieving dictionary schemas ...")
    val dictionarySchemas: Map[String, List[Schema]] = loadSchemas()
    log.info("Pre-processing files ...")
    val data = preProcess(filesPerFolder, s3Bucket)(dictionarySchemas)

    data.foreach({ case (prefix, ndfList) =>
      val outputFolder = prefix.replace(preProcessInput, preProcessOutput)
      val outputPath= s"s3a://$s3Bucket/$outputFolder"

      ndfList.foreach(ndf =>
        transformAndWrite(ndf, outputPath)
      )

      writeSuccessIndicator(s3Bucket, prefix, s3Client);
    })
  }

  private def transformAndWrite(ndf: NamedDataFrame, outputPath: String): Unit = {
    val name = ndf.name

    /*val fileName = name match {
      case "donor" => "donor"
      case "diagnosis" => "diagnosis"
      case "phenotype" => "phenotype"
      case "biospecimen" => "biospecimen"
      case "sampleregistration" => "sample_registration"
      case "treatment" => "treatment"
      case "exposure" => "exposure"
      case "followup" => "follow-up"
      case "familyhistory" => "family-history"
      case "family" => "family"
      case "file" => "file"
      case "study" => "study"
      case _ => throw new RuntimeException(s"Unexpected input file with name: $name")
    }*/

    var df = ndf.dataFrame

    if (name.equals("study")) {
      log.debug("Add meta-data columns to study")
      df = df.withColumn("dictionary_version", lit(ndf.dictionaryVersion))
        .withColumn("study_version", lit(ndf.studyVersion))
        .withColumn("study_version_creation_date", lit(ndf.studyVersionCreationDate))
    }

    val output = s"$outputPath/$name"

    log.info(s"Saving $output ...")

    preProcessFormat match {
      case "tsv" => writeTSV(df, output)
      case "parquet" => writeParquet(df, output)
      case format => throw new RuntimeException(s"Unsupported pre-process format: $format")
    }

  }

  private def writeTSV(df: DataFrame, output: String): Unit = {
    df
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", "\t")
      .mode(SaveMode.Overwrite)
      .csv(output)
  }

  private def writeParquet(df: DataFrame, output: String): Unit = {
    df
      .write
      .mode(SaveMode.Overwrite)
      .parquet(output)
  }

  run()

  spark.stop()

}

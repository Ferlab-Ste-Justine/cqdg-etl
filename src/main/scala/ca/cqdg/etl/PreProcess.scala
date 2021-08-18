package ca.cqdg.etl

import ca.cqdg.etl.model.{NamedDataFrame, S3File}
import ca.cqdg.etl.utils.PreProcessingUtils.{loadSchemas, preProcess}
import ca.cqdg.etl.utils.S3Utils.writeSuccessIndicator
import ca.cqdg.etl.utils.{S3Utils, Schema}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.slf4j
import org.slf4j.LoggerFactory

object PreProcess {

  val preProcessConfig: Config = ConfigFactory.load.getObject("pre-process").toConfig
  val preProcessLogger: String = preProcessConfig.getString("logger")
  val preProcessInput: String = preProcessConfig.getString("input")
  val preProcessOutput: String = preProcessConfig.getString("output")
  val preProcessFormat: String = preProcessConfig.getString("format")

  val log: slf4j.Logger = LoggerFactory.getLogger(preProcessLogger)
  Logger.getLogger(preProcessLogger).setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {
    log.info("Looking for un-processed S3 files ...")
    val filesPerFolder: Map[String, List[S3File]] = S3Utils.loadFileEntries(s3Bucket, preProcessInput, s3Client)

    if (filesPerFolder.nonEmpty) {
      log.info("Retrieving dictionary schemas ...")
      val dictionarySchemas: Map[String, List[Schema]] = loadSchemas()
      log.info(s"Pre-processing of ${filesPerFolder.size} folder(s) ...")
      val data = preProcess(filesPerFolder, s3Bucket)(dictionarySchemas)

      data.foreach({ case (prefix, ndfList) =>
        log.info(s"Pre-processing folder: $prefix")
        val outputFolder = prefix.replace(preProcessInput, preProcessOutput)
        val outputPath = s"s3a://$s3Bucket/$outputFolder"

        ndfList.foreach(ndf =>
          transformAndWrite(ndf, outputPath)
        )

        log.info(s"Successfully pre-processed: $prefix")
        writeSuccessIndicator(s3Bucket, prefix, s3Client);
      })
    }
  }

  private def transformAndWrite(ndf: NamedDataFrame, outputPath: String): Unit = {
    val name = ndf.name
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
      case "tsv" => df.coalesce(1).write.options(tsv_with_headers).mode(SaveMode.Overwrite).csv(output)
      case "parquet" => df.write.mode(SaveMode.Overwrite).parquet(output)
      case format => throw new RuntimeException(s"Unsupported pre-process format: $format")
    }

  }
}

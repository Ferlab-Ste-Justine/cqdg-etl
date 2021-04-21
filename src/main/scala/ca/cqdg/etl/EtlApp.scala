package ca.cqdg.etl

import ca.cqdg.etl.model.{NamedDataFrame, S3File}
import ca.cqdg.etl.utils.EtlUtils.columns.notNullCol
import ca.cqdg.etl.utils.EtlUtils.{getConfiguration, getDataframe}
import ca.cqdg.etl.utils.PreProcessingUtils.preProcess
import ca.cqdg.etl.utils.S3Utils
import ca.cqdg.etl.utils.S3Utils.writeSuccessIndicator
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object EtlApp extends App {

  Logger.getLogger("ferlab").setLevel(Level.INFO)

  implicit val spark = SparkSession
    .builder
    .appName("CQDG ETL")
    .master("local")//TODO: Remove "local" and replace by '[*]'
    .getOrCreate()

  // https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#general_configuration
  // The following env var must be set :
  // SERVICE_ENDPOINT
  // AWS_ACCESS_KEY_ID
  // AWS_SECRET_ACCESS_KEY
  // AWS_DEFAULT_REGION
  val s3Endpoint = getConfiguration("SERVICE_ENDPOINT", "http://localhost:9000")
  val s3Bucket: String = getConfiguration("AWS_BUCKET", "cqdg")

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3Endpoint)
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")

  // Load files that are ready to be processed and apply modifications/validations
  //    e.g.: Add CQDG Id, remove columns that are not part of the dictionary, etc.
  // Returns a list of files per study version (clinical-data/e2adb961-4f58-4e13-a24f-6725df802e2c/11-PLA-STUDY/15)
  val filesPerFolder: Map[String, List[S3File]] = S3Utils.loadFileEntries(s3Bucket, "clinical-data")
  // List of DataFrames per study version (clinical-data/e2adb961-4f58-4e13-a24f-6725df802e2c/11-PLA-STUDY/15)
  val readyToProcess: Map[String, List[NamedDataFrame]] = preProcess(filesPerFolder);

  // Save the modified files in a different folder in order to preserve the original files
  // Then, transform into the right format for the etl-indexer to send to ES.
  import spark.implicits._
  readyToProcess.foreach(entry => {
    val outputPath= s"s3a://${s3Bucket}/clinical-data-etl-indexer"
    val study: DataFrame = getDataframe("study", entry._2).dataFrame
      .select( cols=
        $"*",
        $"study_id" as "study_id_keyword",
        $"short_name" as "short_name_keyword",
        $"short_name" as "short_name_ngrams"
      )
      .withColumn("short_name", notNullCol($"short_name"))
      .as("study")

    val broadcastStudies = spark.sparkContext.broadcast(study)

    Donor.run(broadcastStudies, entry._2, s"$outputPath/donors")
    Study.run(broadcastStudies, entry._2, s"$outputPath/studies")
    File.run(broadcastStudies, entry._2, s"$outputPath/files")

    writeSuccessIndicator(s3Bucket, entry._1);
  });

  spark.stop()
}

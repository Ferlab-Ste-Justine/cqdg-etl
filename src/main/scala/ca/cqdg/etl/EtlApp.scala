package ca.cqdg.etl

import ca.cqdg.etl.EtlUtils.columns.notNullCol
import ca.cqdg.etl.EtlUtils.readCsvFile
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object EtlApp extends App {

  val Array(input, output) = args
  //input  = /home/plaplante/CHUST/projects/cqdg/cqdg-etl/src/test/resources/csv/input
  //output = /home/plaplante/CHUST/projects/cqdg/cqdg-etl/src/test/resources/csv/output

  Logger.getLogger("ferlab").setLevel(Level.ERROR)

  implicit val spark = SparkSession
    .builder
    .appName("CQDG-ETL")
    .master("local")//[*]
    .getOrCreate()

  import spark.implicits._

  val studiesInput = s"$input/study.tsv"

  val study: DataFrame = readCsvFile(studiesInput)
    .select( cols=
      $"study_id",
      $"study_id" as "study_id_keyword",
      $"name",
      notNullCol($"short_name") as "short_name",
      $"short_name" as "short_name_keyword",
      $"short_name" as "short_name_ngrams",
      $"domain",
      $"population"
    ) as "study"

  val broadcastStudies = spark.sparkContext.broadcast(study);

  Donor.run(broadcastStudies, input, s"$output/cases")
  File.run(broadcastStudies, input, s"$output/files")

  spark.stop()
}

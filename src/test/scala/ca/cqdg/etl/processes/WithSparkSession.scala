package ca.cqdg.etl.processes

import org.apache.spark.sql.SparkSession

trait WithSparkSession {

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("CQDG-ETL-TEST")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  sys.addShutdownHook(spark.stop())

}

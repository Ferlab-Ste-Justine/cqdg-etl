package ca.cqdg.etl.commands

import org.apache.spark.sql.SparkSession

object SparkConfig {

  def getSparkSession(isDev: Boolean): SparkSession = {
    val sparkSessionBuilder: SparkSession.Builder = SparkSession
      .builder
      .appName("CQDG-ETL")

    if (isDev) {
      sparkSessionBuilder.master("local")
    }

    val spark = sparkSessionBuilder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    sys.addShutdownHook(spark.stop())

    if (isDev) {
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://localhost:9000")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "minio")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "minio123")
    }

    spark
  }
}

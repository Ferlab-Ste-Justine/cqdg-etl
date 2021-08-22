package ca.cqdg.etl.rework.commands

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

object SparkConfig {

  def getSparkSession(isLocal: Boolean): SparkSession = {
    val sparkSessionBuilder: SparkSession.Builder = SparkSession
      .builder
      .appName("CQDG-ETL")

    if (isLocal) {
      sparkSessionBuilder.master("local")
    }

    val spark = sparkSessionBuilder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    sys.addShutdownHook(spark.stop())

    if (isLocal) {
      val s3Config: Config = ConfigFactory.load.getObject("s3").toConfig
      val s3Endpoint: String = s3Config.getString("endpoint")
      val s3ClientId: String = s3Config.getString("client-id")
      val s3SecretKey: String = s3Config.getString("secret-key")

      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3Endpoint)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3ClientId)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3SecretKey)
    }

    spark
  }
}

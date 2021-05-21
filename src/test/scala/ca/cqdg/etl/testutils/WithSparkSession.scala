package ca.cqdg.etl.testutils

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}

trait WithSparkSession {

  implicit lazy val spark: SparkSession = SparkSession
    .builder
    .appName("CQDG-ETL-TEST")
    .master("local[*]")
    .getOrCreate()


  spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://localhost:9000")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "minioadmin")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "minioadmin")
  spark.sparkContext.setLogLevel("ERROR")

  def withOutputFolder[T](prefix: String)(block: String => T): T = {
    val output: Path = Files.createTempDirectory(prefix)
    try {
      block(output.toAbsolutePath.toString)
    } finally {
      FileUtils.deleteDirectory(output.toFile)
    }
  }
}

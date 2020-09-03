import java.io.File
import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

trait WithSparkSession {
  private val tmp = new File("tmp").getAbsolutePath

  implicit lazy val spark: SparkSession = SparkSession
                                              .builder
                                              .appName("CQDG-ETL-TEST")
                                              .master("local[*]")
                                              .getOrCreate()

  def withOutputFolder[T](prefix: String)(block: String => T): T = {
    val output: Path = Files.createTempDirectory(prefix)
    try {
      block(output.toAbsolutePath.toString)
    } finally {
      FileUtils.deleteDirectory(output.toFile)
    }
  }
}
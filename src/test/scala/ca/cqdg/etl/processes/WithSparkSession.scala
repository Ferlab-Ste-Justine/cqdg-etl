package ca.cqdg.etl.processes

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SparkSession

import java.io.FileOutputStream
import java.nio.file.Files
import java.util.zip.GZIPInputStream

trait WithSparkSession {

  implicit lazy val spark: SparkSession = SparkSession
    .builder
    .appName("CQDG-ETL-TEST")
    .master("local")
    //  When you dealing with less amount of data, you should typically reduce the shuffle partitions
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  sys.addShutdownHook(spark.stop())

  def createTempFolder(fileName: String): String = {
    val tmpFolder = Files.createTempDirectory(fileName).toFile
    val path = tmpFolder.getAbsolutePath
    println(s"Create temp folder $path")
    tmpFolder.deleteOnExit()
    path
  }

  def unZip(resource: String, targetFile: String): Unit = {
    println(s"Unzip resource: $resource into $targetFile")
    val gzis = new GZIPInputStream(getClass.getClassLoader.getResourceAsStream(resource))
    val fos = new FileOutputStream(targetFile)
    IOUtils.copy(gzis, fos)
    IOUtils.closeQuietly(gzis)
    IOUtils.closeQuietly(fos)
  }

}

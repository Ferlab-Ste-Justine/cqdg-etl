package ca.cqdg.etl

import bio.ferlab.datalake.spark3.config.Configuration
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

package object processes {

  // better than scala global executor + keep the App alive until shutdown
  implicit val executorContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  // properly shutdown after app execution
  sys.addShutdownHook(executorContext.shutdown())

  val tsv_with_headers = Map("sep" -> "\t", "header" -> "true")
  val json_multiline = Map("multiline" -> "true")

  def write(sourceId: String, df: DataFrame, conf: Configuration): Unit = {
    val source = conf.getDataset(sourceId)
    val storage = conf.getStorage(source.storageid)
    val outputPath = s"$storage/${source.path}"

    df
      .coalesce(1)
      .write
      .format(source.format.sparkFormat)
      .mode(SaveMode.Overwrite)
      .options(source.writeoptions)
      .partitionBy(source.partitionby:_*)
      .save(outputPath)
  }
}

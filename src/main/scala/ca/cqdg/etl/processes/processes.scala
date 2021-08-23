package ca.cqdg.etl

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
}

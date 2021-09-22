package ca.cqdg.etl.clients.inf

import scala.concurrent.{ExecutionContextExecutorService, Future}

trait IKeycloak {

  def isEnabled(): Boolean

  def createResources(names: Set[String])(implicit executorContext: ExecutionContextExecutorService) : Future[Set[String]]

}

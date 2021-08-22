package ca.cqdg.etl.rework.clients.inf

trait IIdServer {
  def getCQDGIds(payload: String): String
}

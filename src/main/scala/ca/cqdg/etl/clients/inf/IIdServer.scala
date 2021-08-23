package ca.cqdg.etl.clients.inf

trait IIdServer {
  def getCQDGIds(payload: String): String
}

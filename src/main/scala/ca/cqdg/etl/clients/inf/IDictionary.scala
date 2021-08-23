package ca.cqdg.etl.clients.inf

import ca.cqdg.etl.models.Schema

trait IDictionary {
  def loadSchemas(): Map[String, List[Schema]]
}

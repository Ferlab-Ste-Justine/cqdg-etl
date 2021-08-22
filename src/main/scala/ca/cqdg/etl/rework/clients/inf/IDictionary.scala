package ca.cqdg.etl.rework.clients.inf

import ca.cqdg.etl.rework.models.Schema

trait IDictionary {
  def loadSchemas(): Map[String, List[Schema]]
}

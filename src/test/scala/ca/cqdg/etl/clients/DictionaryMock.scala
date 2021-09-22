package ca.cqdg.etl.clients

import ca.cqdg.etl.clients.inf.IDictionary
import ca.cqdg.etl.models.Schema
import com.google.gson.{JsonArray, JsonParser}

import scala.collection.mutable
import scala.io.Source

class DictionaryMock extends IDictionary {

  override def loadSchemas(): Map[String, List[Schema]] = {
    val schemasPerVersion = new mutable.HashMap[String, List[Schema]]
    val dictionaryMock = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("dictionary/dictionary.json")).getLines().mkString
    val jsonResponse: JsonArray = new JsonParser().parse(dictionaryMock).getAsJsonArray
    jsonResponse.forEach(el => {
      val version = el.getAsJsonObject.get("version").getAsString
      val schemaMock = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(s"schema/$version.json")).getLines().mkString
      schemasPerVersion.put(version, new DictionaryClient().getSchemaList(schemaMock))
    })
    schemasPerVersion.toMap
  }

}

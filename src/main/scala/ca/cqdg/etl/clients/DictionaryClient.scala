package ca.cqdg.etl.clients

import ca.cqdg.etl.clients.inf.IDictionary
import ca.cqdg.etl.models.Schema
import ca.cqdg.etl.EtlUtils.sanitize
import com.google.gson.{JsonArray, JsonParser}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.ContentType

import java.net.{URL, URLEncoder}
import scala.collection.mutable

class DictionaryClient extends BaseHttpClient with IDictionary{

  val dictionaryConfig: Config = ConfigFactory.load.getObject("lectern").toConfig
  val dictionaryUsername: String = dictionaryConfig.getString("username")
  val dictionaryPassword: String = dictionaryConfig.getString("password")
  val dictionaryName: String = dictionaryConfig.getString("dictionary-name")
  val dictionaryURL: String = dictionaryConfig.getString("endpoint")

  override def loadSchemas(): Map[String, List[Schema]] = {
    val schemasPerVersion = new mutable.HashMap[String, List[Schema]]

    val (body, status): (Option[String], Int) = dictionaryRequest(s"dictionaries?name=${URLEncoder.encode(dictionaryName, charsetUTF8)}")

    if (200 == status && body.isDefined) {
      val jsonResponse: JsonArray = new JsonParser().parse(body.get).getAsJsonArray
      jsonResponse.forEach(el => {
        val version = el.getAsJsonObject.get("version").getAsString
        schemasPerVersion.put(version, loadSchemaVersion(version))
      })
    } else {
      throw new RuntimeException(s"Failed to retrieve Lectern's versions for $dictionaryName.\n${body.getOrElse("")}")
    }

    schemasPerVersion.toMap
  }

  private def loadSchemaVersion(version: String): List[Schema] = {
    val (body, status): (Option[String], Int) =
      dictionaryRequest(s"dictionaries?name=${URLEncoder.encode(dictionaryName, charsetUTF8)}&version=$version")

    if (200 == status && body.isDefined) {
      getSchemaList(body.get)
    } else {
      throw new RuntimeException(s"Failed to retrieve Lectern's schemas for version $version of $dictionaryName.\n${body.getOrElse("")}")
    }

  }

  def getSchemaList(responseString: String): List[Schema] = {
    val schemas: mutable.MutableList[Schema] = mutable.MutableList()

    val jsonResponse: JsonArray = new JsonParser().parse(responseString).getAsJsonArray
    jsonResponse.forEach(el => {
      val jsonSchemas = el.getAsJsonObject.get("schemas").getAsJsonArray
      jsonSchemas.forEach(x => {
        val entityType = sanitize(x.getAsJsonObject.get("name").getAsString)
        schemas += Schema(entityType, getSchemaFields(entityType, x.getAsJsonObject.get("fields").getAsJsonArray))
      })
    })

    // Add the file schema which is used for mapping the genomic files to the clinical data
    schemas += Schema("file", Seq("submitter_biospecimen_id", "submitter_donor_id", "study_id", "internal_file_id", "file_name",
      "data_category", "data_type", "is_harmonized", "experimental_strategy", "data_access", "file_format", "platform", "variant_class"))

    schemas.toList
  }

  private def getSchemaFields(entityType: String, fields: JsonArray): List[String] = {
    val fieldsList: mutable.MutableList[String] = mutable.MutableList()
    fields.forEach(field => fieldsList += field.getAsJsonObject.get("name").getAsString)
    fieldsList += s"internal_${entityType}_id"
    fieldsList.toList
  }

  private def dictionaryRequest(urlSuffix: String): (Option[String], Int) = {
    val url = new URL(new URL(dictionaryURL), urlSuffix).toString
    val httpRequest = new HttpGet(url)
    httpRequest.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType)
    addBasicAuth(httpRequest, dictionaryUsername, dictionaryPassword)
    executeHttpRequest(httpRequest)
  }
}

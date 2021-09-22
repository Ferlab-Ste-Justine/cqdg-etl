package ca.cqdg.etl.clients

import ca.cqdg.etl.clients.inf.IIdServer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}

class IdServerClient extends BaseHttpClient with IIdServer{

  val idServerConfig: Config = ConfigFactory.load.getObject("id-server").toConfig
  val idServerEndpoint: String = idServerConfig.getString("endpoint")
  val idServerUsername: String = idServerConfig.getString("username")
  val idServerPassword: String = idServerConfig.getString("password")

  override def getCQDGIds(payload: String): String = {
    val url = s"${idServerEndpoint}/batch"

    val httpRequest = new HttpPost(url)
    httpRequest.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType)
    addBasicAuth(httpRequest, idServerUsername, idServerPassword)
    httpRequest.setEntity(new StringEntity(payload))
    val (body, status) = executeHttpRequest(httpRequest)

    if (200 == status && body.isDefined) {
      body.get
    } else {
      throw new RuntimeException(s"Failed to retrieve ids from id-service at ${url}.\n${body.getOrElse("")}")
    }
  }

}

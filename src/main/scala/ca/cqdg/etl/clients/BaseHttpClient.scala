package ca.cqdg.etl.clients

import org.apache.commons.lang3.StringUtils
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHeaders, HttpResponse}

import java.util.Base64

abstract class BaseHttpClient {

  val httpBuilder: HttpClientBuilder = HttpClientBuilder.create()
  val http: CloseableHttpClient = httpBuilder.build()
  val charsetUTF8 = "UTF-8"
  sys.addShutdownHook(http.close())

  def addBasicAuth(httpRequest: HttpRequestBase, username: String, password: String): Unit = {
    if (StringUtils.isNoneBlank(username, password)) {
      val auth = Base64.getEncoder().encodeToString((s"$username:$password").getBytes(charsetUTF8));
      httpRequest.addHeader(HttpHeaders.AUTHORIZATION, s"Basic $auth")
    }
  }

  protected def executeHttpRequest(request: HttpRequestBase): (Option[String], Int) = {
    val response: HttpResponse = http.execute(request)
    val body = Option(response.getEntity).map(e => EntityUtils.toString(e, charsetUTF8))
    // always properly close
    EntityUtils.consumeQuietly(response.getEntity)
    (body, response.getStatusLine.getStatusCode)
  }

}

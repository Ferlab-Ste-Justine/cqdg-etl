package ca.cqdg.etl.utils

import ca.cqdg.etl.utils.EtlUtils.getConfiguration
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}
import sttp.model.{MediaType, StatusCode}

object ExternalApi {

  val idServiceURL: String = getConfiguration("ID_SERVICE_HOST", "http://localhost:5000")

  def getCQDGIds(payload: String): String = {
    val backend = HttpURLConnectionBackend()

    val url = s"${idServiceURL}/batch"

    // response.body : Left(errorMessage), Right(body)
    val response = basicRequest
      .contentType(MediaType.ApplicationJson)
      .body(payload)
      .post(uri"${url}")
      .send(backend)

    backend.close

    if (StatusCode.Ok == response.code && response.body.toString.trim.length > 0) {
      response.body.right.get
    } else {
      throw new RuntimeException(s"Failed to retrieve ids from id-service at ${url}.\n${response.body.left.get}")
    }
  }

}

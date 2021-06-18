package ca.cqdg.etl.utils

import com.typesafe.config.ConfigFactory
import org.keycloak.authorization.client.{AuthzClient, Configuration}
import org.keycloak.representations.idm.authorization.{ResourceRepresentation, ScopeRepresentation}
import org.slf4j.LoggerFactory

import java.util.Collections
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success}

object KeycloakUtils {

  private val log = LoggerFactory.getLogger(this.getClass)
  private val config = ConfigFactory.load.getObject("keycloak").toConfig
  private val keycloakAuthClientConfig = new Configuration(
    config.getString("auth-server-url"),
    config.getString("realm"),
    config.getString("client-id"),
    Collections.singletonMap("secret", config.getString("secret-key")),
    null) // keycloak API will create a default httpClient

  val isEnabled: Boolean = config.getBoolean("settings.enabled")

  def createResources(names: Set[String])(implicit executorContext: ExecutionContextExecutorService) : Future[Set[Option[String]]] = {
    log.info(s"Try to create ${names.size} resources...")
    implicit val keycloakAuthClient: AuthzClient = AuthzClient.create(keycloakAuthClientConfig)
    Future.traverse(names)(name => Future(createResource(name)))
  }

  private def createResource(name: String)(implicit keycloakAuthClient: AuthzClient): Option[String] = {
    val newResource = new ResourceRepresentation()
    newResource.setName(name)
    newResource.setType(s"${config.getString("settings.type-prefix")}${name.toLowerCase}")
    newResource.addScope(new ScopeRepresentation(config.getString("settings.scope")))

    val resourceClient = keycloakAuthClient.protection.resource
    val existingResource = Option(resourceClient.findByName(newResource.getName))

    existingResource match {
      case Some(existing) => {
        log.debug(s"Resource ${name} already exists")
        Some(existing.getId) // could also return None
      }
      case None => {
        val response = resourceClient.create(newResource)
        log.debug(s"New resource created: ${response.getId}")
        Some(response.getId)
      }
    }
  }
}

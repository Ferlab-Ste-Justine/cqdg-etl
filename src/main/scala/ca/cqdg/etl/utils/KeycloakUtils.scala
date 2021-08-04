package ca.cqdg.etl.utils

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.keycloak.authorization.client.{AuthzClient, Configuration}
import org.keycloak.representations.idm.authorization.{ResourceRepresentation, ScopeRepresentation}
import org.slf4j.LoggerFactory

import java.util.Collections
import scala.concurrent.{ExecutionContextExecutorService, Future}

object KeycloakUtils {

  private val config = ConfigFactory.load.getObject("keycloak").toConfig
  private val keycloakAuthClientConfig = new Configuration(
    config.getString("auth-server-url"),
    config.getString("realm"),
    config.getString("client-id"),
    Collections.singletonMap("secret", config.getString("secret-key")),
    null) // keycloak API will create a default httpClient

  private val keycloakLogger = config.getString("logger")
  private val log = LoggerFactory.getLogger(keycloakLogger)
  Logger.getLogger(keycloakLogger).setLevel(Level.INFO)

  val isEnabled: Boolean = config.getBoolean("settings.enabled")

  def createResources(names: Set[String])(implicit executorContext: ExecutionContextExecutorService) : Future[Set[String]] = {
    log.info(s"Try to create ${names.size} resources...")
    implicit val keycloakAuthClient: AuthzClient = AuthzClient.create(keycloakAuthClientConfig)
    Future.traverse(names)(name => Future(createResource(name)))
  }

  private def createResource(name: String)(implicit keycloakAuthClient: AuthzClient): String = {
    val newResource = new ResourceRepresentation()
    newResource.setName(name)
    newResource.setType(s"${config.getString("settings.type-prefix")}${name.toLowerCase}")
    newResource.addScope(new ScopeRepresentation(config.getString("settings.scope")))

    val resourceClient = keycloakAuthClient.protection.resource
    val existingResource = Option(resourceClient.findByName(newResource.getName))

    existingResource.getOrElse(resourceClient.create(newResource)).getId
  }
}

package ca.cqdg.etl.processes

import ca.cqdg.etl.clients.inf.IKeycloak
import org.apache.log4j.{Level, Logger}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j
import org.slf4j.LoggerFactory

import java.nio.file.Files
import scala.concurrent.{ExecutionContextExecutorService, Future}

class ProcessETLTest extends AnyFunSuite with WithSparkSession {

  val log: slf4j.Logger = LoggerFactory.getLogger("process-test")
  Logger.getLogger("process-test").setLevel(Level.INFO)

  test("compare expected / transformed files") {

    val keycloakMock = new IKeycloak {
      override def isEnabled(): Boolean = true
      override def createResources(names: Set[String])(implicit executorContext: ExecutionContextExecutorService): Future[Set[String]] = {
        assert(names.size == 1 && names.head.equals("internal_id")) // we have only one distinct internal id
        Future(Set("internal_id"))
      }
    }

    val input = "src/test/resources/clinical-data-with-ids"
    val ontology = "src/test/resources/ontology"
    val output = Files.createTempDirectory("cqdg-etl-process-test").toFile.getAbsolutePath

    val config = new ProcessETLTestConfig(input, ontology, output).processETLConfig
    val etl = new ProcessETL(keycloakMock)(spark, config)

    val data = etl.extract()
    assert(data.size == 16)

    val (studies, donors, files) = etl.transform(data)

    /*log.info(s"Current output folder: $output")

    etl.load(studies, donors, files)*/

    // TODO assert
    /*
      collect (sous forme d'une classe model) sous forme de liste scala + comparer avec autre liste
      datalake dataframe => generateur de model (à trouver)
     */

  }

}

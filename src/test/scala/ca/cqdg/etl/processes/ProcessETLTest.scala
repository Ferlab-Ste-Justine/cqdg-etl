package ca.cqdg.etl.processes

import ca.cqdg.etl.clients.inf.IKeycloak
import org.apache.spark.sql.{DataFrame, Encoder}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.{ExecutionContextExecutorService, Future}

class ProcessETLTest extends AnyFunSuite with WithSparkSession {

  test("compare expected / transformed files") {

    val keycloakMock = new IKeycloak {
      override def isEnabled(): Boolean = true
      override def createResources(names: Set[String])(implicit executorContext: ExecutionContextExecutorService): Future[Set[String]] = {
        assert(names.size == 1 && names.head.equals("file_internal_id_1")) // we have only one distinct internal id
        Future(names)
      }
    }

    val input = getClass.getClassLoader.getResource("clinical-data-with-ids").toString
    val ontology = createTempFolder("cqdg-etl-process-test-ontology")
    val output = createTempFolder("cqdg-etl-process-test")

    // Spark .gz is very low, unzip the file manually first
    unZip("ontology/duo_code_terms.json.gz", s"$ontology/duo_code_terms.json")
    unZip("ontology/hpo_terms.json.gz", s"$ontology/hpo_terms.json")
    unZip("ontology/icd_terms.json.gz", s"$ontology/icd_terms.json")
    unZip("ontology/mondo_terms.json.gz", s"$ontology/mondo_terms.json")

    val config = new ProcessETLTestConfig(input, ontology, output).processETLConfig
    val etl = new ProcessETL(keycloakMock)(spark, config)

    val data = etl.extract()
    assert(data.size == 16)

    val (studies, donors, files) = etl.transform(data)

    //etl.load(studies, donors, files)

    // following is how to create expected scala class from transformed dataframes
    /*val packageName = "ca.cqdg.etl.validation.process"
    val rootFolder = "src/test/scala/"
    ClassGenerator.writeCLassFile(packageName, "DonorsExpected", donors, rootFolder)*/

    /*import spark.implicits._

    println("Assert studies ...")
    val studiesTransformed = getDataFrameResultAs[StudiesExpected](studies)
    assert(studiesTransformed.equals(StudiesExpected()))*/

    // TODO assert transformed ...

  }

  private def getDataFrameResultAs[U: Encoder](df: DataFrame): U= {
    df.as[U].collect().head
  }

}

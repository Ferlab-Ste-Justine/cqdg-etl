package ca.cqdg.etl.processes

import ca.cqdg.etl.clients.{DictionaryMock, IdServerMock}
import ca.cqdg.etl.models.NamedDataFrame
import org.apache.log4j.{Level, Logger}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Files
import scala.io.Source

class PreProcessETLTest extends AnyFunSuite with WithSparkSession {

  val log: slf4j.Logger = LoggerFactory.getLogger("pre-process-test")
  Logger.getLogger("pre-process-test").setLevel(Level.INFO)

  test("compare expected / transformed files") {

    val input = "src/test/resources/clinical-data"
    val output = Files.createTempDirectory("cqdg-etl-pre-process-test").toFile.getAbsolutePath

    val config = new PreProcessETLTestConfig(input, output).preProcessETLConfig
    val etl = new PreProcessETL(new DictionaryMock, new IdServerMock)(spark, config)

    val data = etl.extract()
    assert(data.size == 13)

    val transformed = etl.transform(data)
    assert(transformed.size == 12)

    log.info(s"Current output folder: $output")

    etl.load(transformed)

    // check every lines of expected / transformed
    transformed.foreach(assertOutput(_, output))
  }

  private def assertOutput(ndf: NamedDataFrame, outputFolder: String): Unit = {
    log.info(s"Assert content ${ndf.name}")
    val expectedFile = s"clinical-data-with-ids/${ndf.name}.tsv"
    val expected = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(expectedFile)).getLines().toList
    val transformedFile = findTransformedFile(ndf.name, outputFolder)
    val transformed = Source.fromFile(transformedFile).getLines().toList
    expected.zipWithIndex.foreach({ case(line, i) =>
      if (!transformed.contains(line))
        fail(s"Expected: $expectedFile line: ${i+1} not found in transformed: $transformedFile\n$line")
    })
    transformed.zipWithIndex.foreach({ case(line, i) =>
      if (!expected.contains(line))
        fail(s"Transformed: $transformedFile line: ${i+1} not found in expected: $expectedFile\n$line")
    })
  }

  private def findTransformedFile(name: String, outputFolder: String): String = {
    val outputFiles = new File(s"$outputFolder/$name").listFiles()
    outputFiles.find(file => file.getName.endsWith(".csv")).get.getAbsolutePath
  }

}

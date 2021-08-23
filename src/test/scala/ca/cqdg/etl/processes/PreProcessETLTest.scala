package ca.cqdg.etl.processes

import bio.ferlab.datalake.spark3.config.Configuration
import ca.cqdg.etl.clients.DictionaryClient
import ca.cqdg.etl.clients.inf.{IDictionary, IIdServer}
import ca.cqdg.etl.models.{NamedDataFrame, Schema}
import com.google.gson.{JsonArray, JsonParser}
import org.apache.spark.sql.{SaveMode}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files
import scala.collection.mutable
import scala.io.Source

class PreProcessETLTest extends AnyFunSuite with WithSparkSession{

  test("compare expected / transformed files") {

    val dictionaryMock = new IDictionary {
      override def loadSchemas(): Map[String, List[Schema]] = {
        val schemasPerVersion = new mutable.HashMap[String, List[Schema]]
        val dictionaryMock = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("dictionary/dictionary.json")).getLines().mkString
        val jsonResponse: JsonArray = new JsonParser().parse(dictionaryMock).getAsJsonArray
        jsonResponse.forEach(el => {
          val version = el.getAsJsonObject.get("version").getAsString
          val schemaMock = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("schema/schema.json")).getLines().mkString
          schemasPerVersion.put(version, new DictionaryClient().getSchemaList(schemaMock))
        })
        schemasPerVersion.toMap
      }
    }

    val idServerMock = new IIdServer {
      override def getCQDGIds(payload: String): String = {
        val response = new StringBuilder()
        val json = new JsonParser().parse(payload).getAsJsonObject.entrySet()
        assert(json.size() > 0)
        json.forEach(el => {
          response.append(String.format(s"{'hash':'${el.getKey}','internal_id':'internal_id'},"))
        })
        s"[${response.deleteCharAt(response.size-1).toString}]"
      }
    }

    val input = "src/test/resources/clinical-data"
    val output = Files.createTempDirectory("cqdg-etl-pre-process-test").toFile.getAbsolutePath
    val config = new PreProcessETLConfig(input, output).preProcessETLConfig
    val etl = new PreProcessETL(dictionaryMock, idServerMock)(spark, config)
    val data = etl.extract()
    assert(data.size == 13)
    val transformed = etl.transform(data)
    assert(transformed.size == 12)
    // custom write into CSV
    transformed.foreach(ndf => writeCSV(ndf, config))
    // check very lines of expected / transformed
    transformed.foreach(assertOutput(_, output))
  }

  private def writeCSV(ndf: NamedDataFrame, conf: Configuration): Unit = {
    val source = conf.getDataset(s"${ndf.name}-with-ids")
    val storage = conf.getStorage(source.storageid)
    val outputPath = s"$storage/${source.path}"

    ndf.dataFrame
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(tsv_with_headers)
      .csv(outputPath)
  }

  private def assertOutput(ndf: NamedDataFrame, outputFolder: String): Unit = {
    val expectedFile = s"clinical-data-with-ids/${ndf.name}.tsv"
    val expected = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(expectedFile)).getLines().toList
    val transformedFile = findTransformedFile(ndf.name, outputFolder)
    val transformed = Source.fromFile(findTransformedFile(ndf.name, outputFolder)).getLines().toList
    expected.zipWithIndex.foreach({ case(line, i) =>
      if (!transformed.contains(line))
        fail(s"Expected: $expectedFile line not found in transformed: $transformedFile\n$line")
    })
    transformed.zipWithIndex.foreach({ case(line, i) =>
      if (!expected.contains(line))
        fail(s"Transformed: $transformedFile line not found in expected: $expectedFile\n$line")
    })
  }

  private def findTransformedFile(name: String, outputFolder: String): String = {
    val outputFiles = new File(s"$outputFolder/$name").listFiles()
    outputFiles.find(file => file.getName.endsWith(".csv")).get.getAbsolutePath
  }

}

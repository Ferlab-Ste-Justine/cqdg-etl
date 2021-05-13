package ca.cqdg.etl

import ca.cqdg.etl.model.{NamedDataFrame, S3File}
import ca.cqdg.etl.test.util.TestData.hashCodesList
import ca.cqdg.etl.test.util.WithSparkSession
import ca.cqdg.etl.utils.{EtlUtils, PreProcessingUtils, S3Utils, Schema}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets

class DonorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll with WithSparkSession
//  with DockerTestKit with DockerKitSpotify
//  with MinioS3MockService
{

  //  implicit val pc: PatienceConfig = PatienceConfig(Span(20, Seconds), Span(1, Second))

  val CLINDATA_BUCKET = "cqdg"

//  override def beforeAll(): Unit = {
  val s3Credential = new BasicAWSCredentials("minioadmin", "minioadmin")
  val s3Client: AmazonS3 = AmazonS3ClientBuilder
    .standard()
    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:9000", Regions.US_EAST_1.name()))
    .withPathStyleAccessEnabled(true)
    .withCredentials(new AWSStaticCredentialsProvider(s3Credential))
    .build()

  val inputDirectoryData = new File("../cqdg-etl/src/test/resources/tsv/input")
  val inputDirectoryOntology = new File("../cqdg-etl/src/test/resources/ontology_input")

  if (!s3Client.doesBucketExistV2(CLINDATA_BUCKET)) {
    s3Client.createBucket(CLINDATA_BUCKET)

    if (inputDirectoryData.exists && inputDirectoryData.isDirectory) {
      val list =  inputDirectoryData.listFiles.filter(_.isFile).map(_.getName).toList
      list.foreach(fn => {
        val file = new File(s"../cqdg-etl/src/test/resources/tsv/input/$fn")
        s3Client.putObject(
          CLINDATA_BUCKET,
          s"clinical-data/$fn",
          file
        )
      })
    }
    if (inputDirectoryOntology.exists && inputDirectoryOntology.isDirectory) {
      val list =  inputDirectoryOntology.listFiles.filter(_.isFile).map(_.getName).toList
      list.foreach(fn => {
        val file = new File(s"../cqdg-etl/src/test/resources/ontology_input/$fn")
        s3Client.putObject(
          CLINDATA_BUCKET,
          s"ontology-input/$fn",
          file
        )
      })
    }
  }

  val filesPerFolder: Map[String, List[S3File]] = S3Utils.loadFileEntries(CLINDATA_BUCKET, "clinical-data", s3Client)

  val ontologyTermFiles: Seq[S3File] = S3Utils.loadFileEntry(CLINDATA_BUCKET, "ontology-input", s3Client)
  val hpoFile: S3File = ontologyTermFiles.find(f => f.filename == "hpo_terms.json").get
  val hpoTermDf: DataFrame = spark.read.json(s"s3a://cqdg/${hpoFile.key}")
  hpoTermDf.show(false)

  val schemaJsonFile = new File("../cqdg-etl/src/test/resources/schema/schema.json")


  val schemaList: List[Schema] = PreProcessingUtils.getSchemaList(FileUtils.readFileToString(schemaJsonFile, StandardCharsets.UTF_8))

  val dictionarySchemas: Map[String, List[Schema]] = Map("5.44" -> schemaList)

  val hashString: String = "[" + hashCodesList.map(l => s"""{"hash":"$l","internal_id":"123"}""").mkString(",") + "]"
  val mockBuildIds: String => String = (_: String) => hashString

  val readyToProcess: Map[String, List[NamedDataFrame]] =
    PreProcessingUtils.preProcess(filesPerFolder, CLINDATA_BUCKET, mockBuildIds)(dictionarySchemas)



  "Donors" should "map hpo terms per donors" in {
    readyToProcess.foreach(x => {
      val broadcastDf = EtlUtils.broadcastStudies(x._2)
      val df = Donor.build(broadcastDf, x._2)
      df.show(false)
      println("TOTO")
    })

    1 shouldBe 1
  }
}

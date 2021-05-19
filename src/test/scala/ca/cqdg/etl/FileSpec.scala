package ca.cqdg.etl

import ca.cqdg.etl.model.{NamedDataFrame, S3File}
import ca.cqdg.etl.test.util.TestData.hashCodesList
import ca.cqdg.etl.test.util.WithSparkSession
import ca.cqdg.etl.testutils.model.PHENOTYPES
import ca.cqdg.etl.utils.PreProcessingUtils.getOntologyDfs
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

class FileSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with WithSparkSession
{
  val CLINDATA_BUCKET = "cqdg"

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

  val schemaJsonFile = new File("../cqdg-etl/src/test/resources/schema/schema.json")


  val schemaList: List[Schema] = PreProcessingUtils.getSchemaList(FileUtils.readFileToString(schemaJsonFile, StandardCharsets.UTF_8))

  val dictionarySchemas: Map[String, List[Schema]] = Map("5.44" -> schemaList)

  val hashString: String = "[" + hashCodesList.map(l => s"""{"hash":"$l","internal_id":"123"}""").mkString(",") + "]"
  val mockBuildIds: String => String = (_: String) => hashString

  val readyToProcess: Map[String, List[NamedDataFrame]] =
    PreProcessingUtils.preProcess(filesPerFolder, CLINDATA_BUCKET, mockBuildIds)(dictionarySchemas)

  val ontologyDfs: Map[String, DataFrame] = getOntologyDfs(ontologyTermFiles)

  "File" should "map hpo terms per file" in {
    import spark.implicits._

    val broadcastDf = EtlUtils.broadcastStudies(readyToProcess.head._2)
    val df = File.build(broadcastDf, readyToProcess.head._2, ontologyDfs)
    val phenotypesTestFile = df.filter($"file_name_keyword" === "I21OuzF.cram").select(col = "phenotypes").as[Seq[PHENOTYPES]].collect().head

    //Fixme, age at phenotype for same phenotypes should be in a Set (no duplicates)
    phenotypesTestFile should contain theSameElementsAs Seq(
      PHENOTYPES(
        `id` = "HP:0001513",
        `name` = "Obesity",
        `parents` = Seq("Increased body weight (HP:0004324)"),
        `age_at_phenotype` = 32,
        `phenotype_observed_bool` = true,
        `is_leaf` = false,
        `is_tagged` = true
      ),
      PHENOTYPES(
        `id` = "HP:0004324",
        `name` = "Increased body weight",
        `parents` = Seq("Abnormality of body weight (HP:0004323)"),
        `age_at_phenotype` = 32,
      ),
      PHENOTYPES(
        `id` = "HP:0004323",
        `name` = "Abnormality of body weight",
        `parents` = Seq("Growth abnormality (HP:0001507)"),
        `age_at_phenotype` = 32,
      ),
      PHENOTYPES(
        `id` = "HP:0001507",
        `name` = "Growth abnormality",
        `parents` = Seq("Phenotypic abnormality (HP:0000118)"),
        `age_at_phenotype` = 32
      ),
      PHENOTYPES(
        `id` = "HP:0000118",
        `name` = "Phenotypic abnormality",
        `parents` = Seq("All (HP:0000001)"),
        `age_at_phenotype` = 32
      ),
      PHENOTYPES(
        `id` = "HP:0000001",
        `name` = "All",
        `parents` = Nil,
        `age_at_phenotype` = 32
      )
    )
  }
}

package ca.cqdg.etl

import ca.cqdg.etl.model.{NamedDataFrame, S3File}
import ca.cqdg.etl.testutils.TestData.hashCodesList
import ca.cqdg.etl.testutils.WithSparkSession
import ca.cqdg.etl.testutils.model.PHENOTYPES
import ca.cqdg.etl.utils.PreProcessingUtils.getOntologyDfs
import ca.cqdg.etl.utils.{EtlUtils, PreProcessingUtils, S3Utils, Schema}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.commons.io.FileUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets

class DonorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with WithSparkSession
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

  val broadcastDf: Broadcast[DataFrame] = EtlUtils.broadcastStudies(readyToProcess.head._2)
  val df: DataFrame = Donor.build(broadcastDf, readyToProcess.head._2, ontologyDfs)

  "Donors" should "map observed hpo terms per donors" in {
    import spark.implicits._

    val phenotypesForDonor14 = df.filter($"submitter_donor_id" === "PT00014").select(col = "observed_phenotypes").as[Seq[PHENOTYPES]].collect().head

    phenotypesForDonor14 should contain theSameElementsAs Seq(
      PHENOTYPES(
        `phenotype_id` = "HP:0000501",
        `name` = "Glaucoma",
        `parents` = Seq("Abnormal eye physiology (HP:0012373)"),
        `age_at_event` = Set(63),
        `is_leaf` = false,
        `is_tagged` = true
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0100279",
        `name` = "Ulcerative colitis",
        `parents` = Seq("Chronic colitis (HP:0100281)"),
        `age_at_event` = Set(56),
        `is_leaf` = true,
        `is_tagged` = true
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0012373",
        `name` = "Abnormal eye physiology",
        `parents` = Seq("Abnormality of the eye (HP:0000478)"),
        `age_at_event` = Set(63),
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0000478",
        `name` = "Abnormality of the eye",
        `parents` = Seq("Phenotypic abnormality (HP:0000118)"),
        `age_at_event` = Set(63)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0000118",
        `name` = "Phenotypic abnormality",
        `parents` = Seq("All (HP:0000001)"),
        `age_at_event` = Set(56, 63)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0025032",
        `name` = "Abnormality of digestive system physiology",
        `parents` = Seq("Abnormality of the digestive system (HP:0025031)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0011024",
        `name` = "Abnormality of the gastrointestinal tract",
        `parents` = Seq("Abnormality of the digestive system (HP:0025031)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0002583",
        `name` = "Colitis",
        `parents` = Seq("Inflammation of the large intestine (HP:0002037)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0002037",
        `name` = "Inflammation of the large intestine",
        `parents` = Seq("Abnormal large intestine morphology (HP:0002250)", "Gastrointestinal inflammation (HP:0004386)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0002715",
        `name` = "Abnormality of the immune system",
        `parents` = Seq("Phenotypic abnormality (HP:0000118)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0000001",
        `name` = "All",
        `parents` = Nil,
        `age_at_event` = Set(56, 63)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0025031",
        `name` = "Abnormality of the digestive system",
        `parents` = Seq("Phenotypic abnormality (HP:0000118)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0012718",
        `name` = "Morphological abnormality of the gastrointestinal tract",
        `parents` = Seq("Abnormality of the gastrointestinal tract (HP:0011024)", "Abnormality of digestive system morphology (HP:0025033)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0004386",
        `name` = "Gastrointestinal inflammation",
        `parents` = Seq("Increased inflammatory response (HP:0012649)", "Functional abnormality of the gastrointestinal tract (HP:0012719)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0012649",
        `name` = "Increased inflammatory response",
        `parents` = Seq("Abnormal inflammatory response (HP:0012647)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0010978",
        `name` = "Abnormality of immune system physiology",
        `parents` = Seq("Abnormality of the immune system (HP:0002715)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0012719",
        `name` = "Functional abnormality of the gastrointestinal tract",
        `parents` = Seq("Abnormality of the gastrointestinal tract (HP:0011024)", "Abnormality of digestive system physiology (HP:0025032)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0002250",
        `name` = "Abnormal large intestine morphology",
        `parents` = Seq("Abnormal intestine morphology (HP:0002242)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0012647",
        `name` = "Abnormal inflammatory response",
        `parents` = Seq("Abnormality of immune system physiology (HP:0010978)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0100281",
        `name` = "Chronic colitis",
        `parents` = Seq("Colitis (HP:0002583)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0002242",
        `name` = "Abnormal intestine morphology",
        `parents` = Seq("Morphological abnormality of the gastrointestinal tract (HP:0012718)"),
        `age_at_event` = Set(56)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0025033",
        `name` = "Abnormality of digestive system morphology",
        `parents` = Seq("Abnormality of the digestive system (HP:0025031)"),
        `age_at_event` = Set(56)
      )
    )
  }

  "Donors" should "map non observed hpo terms per donors" in {
    import spark.implicits._

    val phenotypesForDonor14 = df.filter($"study_id" === "ST0001" && $"submitter_donor_id" === "PT00014").select(col = "non_observed_phenotypes").as[Seq[PHENOTYPES]].collect().head

    phenotypesForDonor14 should contain theSameElementsAs Seq(
      PHENOTYPES(
        `phenotype_id` = "HP:0025031",
        `name` = "Abnormality of the digestive system",
        `parents` = Seq("Phenotypic abnormality (HP:0000118)"),
        `age_at_event` = Set(54),
        `is_leaf` = false,
        `is_tagged` = true
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0000118",
        `name` = "Phenotypic abnormality",
        `parents` = Seq("All (HP:0000001)"),
        `age_at_event` = Set(54)
      ),
      PHENOTYPES(
        `phenotype_id` = "HP:0000001",
        `name` = "All",
        `parents` = Nil,
        `age_at_event` = Set(54)
      )
    )
  }
}

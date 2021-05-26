package ca.cqdg.etl

import ca.cqdg.etl.model.{NamedDataFrame, S3File}
import ca.cqdg.etl.testutils.TestData.hashCodesList
import ca.cqdg.etl.testutils.model._
import ca.cqdg.etl.testutils.{WithSparkSession, testConf}
import ca.cqdg.etl.utils.EtlUtils.{getDataframe, loadAll}
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

class FileIndexSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with WithSparkSession {
  import spark.implicits._
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


  val studyNDF = getDataframe("study", readyToProcess.head._2)
  val study: DataFrame =
    studyNDF.dataFrame
      .select(cols =
        $"*",
        $"study_id" as "study_id_keyword",
        $"short_name" as "short_name_keyword",
        $"short_name" as "short_name_ngrams"
      )
      .as("study")



  "File" should "transform data in expected format" in {

    val ontologyDfs =
      Map(
        "hpo" -> Seq(HpoTermsInput()).toDF(),
        "mondo" -> Seq(MondoTermsInput()).toDF()
      )

    val inputData = Map(
      "hpo" -> Seq(HpoTermsInput()).toDF(),
      "mondo" -> Seq(MondoTermsInput()).toDF(),
      "donor" -> Seq(DonorOutput(`submitter_donor_id` = "PT00060")).toDF(),
      "diagnosisPerDonorAndStudy" -> Seq(DonorDiagnosisOutput(`submitter_donor_id` = "PT00060")).toDF(),
      "phenotypesPerDonorAndStudy" -> Seq(PhenotypeWithHpoOutput(`study_id` = "ST0003", `submitter_donor_id` = "PT00060")).toDF(),
      "biospecimenWithSamples" -> Seq(BiospecimenOutput(`submitter_biospecimen_id` = "BS00001")).toDF(),
      "file" -> Seq(FileInput(`submitter_donor_id` = "PT00060", `submitter_biospecimen_id` = Some("BS00001"))).toDF()
    )


    val df = new FileIndex(study, studyNDF, inputData)(testConf).transform(inputData)

    val expectedResult =
      FileIndexOutput(
        "zplTjYga3.pdf", "zplTjYga3.pdf", "ST0003", "zplTjYga3.pdf", "Clinical", "Pathological report", false, None, "Controled", "pdf", None, 17.254430403010716,
        List(STUDY("Study on unusual cancer phenotypes", "Cancer", "Hereditary tumors; Exome sequencing ", "Study3", "Pediatric and Adult", "ST3", "ST3", "ST3", "ST0003", "ST0003")), "no-data",
        List(DONORS("57", "5/10/2009",
          List(DIAGNOSES(52, "Clinical",
            List(FOLLOW_UPS(81, "Stable", "FU00060")), true, "Yes", "Yes", "M1d(0)", "N2", "stage iia", "DI00060", "Tis(LCIS)",
            List(TREATMENTS("TX00134", "Unknown", "Unknown", "Partial Response", "Stem cell transplant")))),
          "2/21/1951", "NO", "French Canadian ",
          List(FILE_EXPOSURE_EXPOSURE("H4V", "Alcohol consumption unknown", "Current every day smoker        ")),
          List(FILE_EXPOSURE_EXPOSURE("H4V", "Alcohol consumption unknown", "Current every day smoker        ")),
          List(FAMILYCONDITIONS("Yes", 53, "Lung cancer", "paternal aunt", "FC00134")),
          List(FAMILYHISTORY("Yes", 48, "Multiple sclerosis", "maternal aunt", "FC00060")), "NO",
          List(FILES("Controled", "Sequencing reads", "Aligned reads", "WGS", "cram", "uK9WHQ0.cram", "uK9WHQ0.cram", "uK9WHQ0.cram", 13.02056279174796, None, true, "Illumina", "BS00060")),
          "Female", "YES", "Not applicable", "Not applicable", "NO", "NO", "YES",
          List(PHENOTYPES("HP:0001513", "Obesity", List("Increased body weight HP:0004324"), 54, true, false, false)), "NO",
          List(STUDY("population-based cohort focusing on complex conditions", "General Health", "Common chronic disorders; Prospective cohort; Reference genome", "Study1",
            "Adult", "ST1", "ST1", "ST1", "ST0001", "ST0001")), "PT00060", "alive")),
        List(BIOSPECIMEN("BS00001", "DI00001", "11/22/2009", "Normal", "No", None, "Acute myocardial infarction", "Cryopreservation - other", "Frozen in -70 freezer",
          "Yes", "Blood derived - peripheral blood", "Normal", "C42.0: Blood",
          List(SAMPLES("SA00001", "Total DNA")))), null,
        List(
          PHENOTYPES("HP:0001694", "Right-to-left shunt", List("Cardiac shunt (HP:0001693)"), 63, true, true, true),
          PHENOTYPES("HP:0001626", "Abnormality of the cardiovascular system", List("Phenotypic abnormality (HP:0000118)"), 63, true, false, false)),
        "5.44", "1.0", "2020/05/01")

    df.as[FileIndexOutput].collect().head.copy(`file_size` = 1) shouldBe expectedResult.copy(`file_size` = 1)

  }

  "File" should "map hpo terms per file" in {

    val (donor, diagnosisPerDonorAndStudy, phenotypesPerDonorAndStudy, biospecimenWithSamples, file, _) = loadAll(readyToProcess.head._2)(getOntologyDfs(ontologyTermFiles))
    val inputData: Map[String, DataFrame] = Map(
        "donor" -> donor,
        "diagnosisPerDonorAndStudy" -> diagnosisPerDonorAndStudy,
        "phenotypesPerDonorAndStudy" -> phenotypesPerDonorAndStudy,
        "biospecimenWithSamples" -> biospecimenWithSamples,
        "file" -> file
    )

    val job = new FileIndex(study, studyNDF, inputData)(testConf)

    val df = job.transform(job.extract())

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

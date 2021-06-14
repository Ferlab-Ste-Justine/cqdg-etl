package ca.cqdg.etl

import ca.cqdg.etl.EtlApp.ontologyDfs
import ca.cqdg.etl.model.{NamedDataFrame, S3File}
import ca.cqdg.etl.testutils.TestData.hashCodesList
import ca.cqdg.etl.testutils.WithSparkSession
import ca.cqdg.etl.testutils.model._
import ca.cqdg.etl.utils.EtlUtils.{getDataframe, loadAll}
import ca.cqdg.etl.utils.PreProcessingUtils.getOntologyDfs
import ca.cqdg.etl.utils.{PreProcessingUtils, S3Utils, Schema}
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

class DonorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with WithSparkSession
{
  import spark.implicits._
  val CLINDATA_BUCKET = "cqdg"

  val s3Credential = new BasicAWSCredentials("minio", "minio123")
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

  val dictionarySchemas: Map[String, List[Schema]] = Map("5.57" -> schemaList)

  val hashString: String = "[" + hashCodesList.map(l => s"""{"hash":"$l","internal_id":"123"}""").mkString(",") + "]"
  val mockBuildIds: String => String = (_: String) => hashString

  val readyToProcess: Map[String, List[NamedDataFrame]] =
    PreProcessingUtils.preProcess(filesPerFolder, CLINDATA_BUCKET, mockBuildIds)(dictionarySchemas)

  val ontologyDfs: Map[String, DataFrame] = getOntologyDfs(ontologyTermFiles)

  val studyNDF: NamedDataFrame = getDataframe("study", readyToProcess.head._2)

  val study: DataFrame =
    studyNDF.dataFrame
      .select(cols =
        $"*",
        $"study_id" as "study_id_keyword",
        $"short_name" as "short_name_keyword",
        $"short_name" as "short_name_ngrams"
      )
      .as("study")


  val (_,donor, diagnosisPerDonorAndStudy, phenotypesPerStudyIdAndDonor,
  biospecimenWithSamples, file, _, _, _, _, _) = loadAll(readyToProcess.head._2)(getOntologyDfs(ontologyTermFiles))

  val inputData = Map(
    "donor" -> donor,
    "diagnosisPerDonorAndStudy" -> diagnosisPerDonorAndStudy,
    "phenotypesPerStudyIdAndDonor" -> phenotypesPerStudyIdAndDonor,
    "biospecimenWithSamples" -> Seq(BiospecimenOutput(`submitter_biospecimen_id` = "BS00001")).toDF(),
    "dataAccess" -> Seq(DataAccessInput(`entity_type` = "donor", `entity_id` = "PT00444")).toDF(),
    "treatmentsPerDonorAndStudy" -> Seq(TreatmentInput()).toDF(),
    "exposuresPerDonorAndStudy" -> Seq(ExposureInput()).toDF(),
    "followUpsPerDonorAndStudy" -> Seq(FollowUpInput()).toDF(),
    "familyHistoryPerDonorAndStudy" -> Seq(FamilyHistoryInput()).toDF(),
    "familyRelationshipPerDonorAndStudy" -> Seq(FamilyRelationshipInput()).toDF(),
    "file" -> Seq(FileInput(`submitter_donor_id` = "PT00060", `submitter_biospecimen_id` = Some("BS00001"))).toDF(),
  )

  val df: DataFrame = Donor.build(study, studyNDF, inputData, ontologyDfs("duo_code"))

  "Donors" should "map hpo terms per donors" in {
    import spark.implicits._

    val phenotypesForDonor14 = df.filter($"submitter_donor_id" === "PT00014").select(col = "observed_phenotypes").as[Seq[ONTOLOGY_TERM]].collect().head

    val phenotypesForDonor14TaggedDf =  df.filter($"submitter_donor_id" === "PT00014")
      .select($"observed_phenotype_tagged")

    val phenotypesForDonor14Tagged = phenotypesForDonor14TaggedDf.as[Seq[ONTOLOGY_TERM]].collect().head
    val main_category14 = phenotypesForDonor14TaggedDf.select($"observed_phenotype_tagged.main_category")

    main_category14.as[Seq[String]].collect().head should contain theSameElementsAs Seq(
      "Abnormality of the immune system (HP:0002715)", "Abnormality of the eye (HP:0000478)")

    phenotypesForDonor14Tagged should contain theSameElementsAs Seq(
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0000501",
        `name` = "Glaucoma",
        `parents` = Seq("Abnormal eye physiology (HP:0012373)"),
        `age_at_event` = Set(63),
        `is_leaf` = false,
        `is_tagged` = true
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0100279",
        `name` = "Ulcerative colitis",
        `parents` = Seq("Chronic colitis (HP:0100281)"),
        `age_at_event` = Set(56),
        `is_leaf` = true,
        `is_tagged` = true
      ),
    )

    phenotypesForDonor14 should contain theSameElementsAs Seq(
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0000501",
        `name` = "Glaucoma",
        `parents` = Seq("Abnormal eye physiology (HP:0012373)"),
        `age_at_event` = Set(63),
        `is_leaf` = false,
        `is_tagged` = true
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0100279",
        `name` = "Ulcerative colitis",
        `parents` = Seq("Chronic colitis (HP:0100281)"),
        `age_at_event` = Set(56),
        `is_leaf` = true,
        `is_tagged` = true
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0012373",
        `name` = "Abnormal eye physiology",
        `parents` = Seq("Abnormality of the eye (HP:0000478)"),
        `age_at_event` = Set(63),
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0000478",
        `name` = "Abnormality of the eye",
        `parents` = Seq("Phenotypic abnormality (HP:0000118)"),
        `age_at_event` = Set(63)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0000118",
        `name` = "Phenotypic abnormality",
        `parents` = Seq("All (HP:0000001)"),
        `age_at_event` = Set(56, 63)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0025032",
        `name` = "Abnormality of digestive system physiology",
        `parents` = Seq("Abnormality of the digestive system (HP:0025031)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0011024",
        `name` = "Abnormality of the gastrointestinal tract",
        `parents` = Seq("Abnormality of the digestive system (HP:0025031)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0002583",
        `name` = "Colitis",
        `parents` = Seq("Inflammation of the large intestine (HP:0002037)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0002037",
        `name` = "Inflammation of the large intestine",
        `parents` = Seq("Abnormal large intestine morphology (HP:0002250)", "Gastrointestinal inflammation (HP:0004386)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0002715",
        `name` = "Abnormality of the immune system",
        `parents` = Seq("Phenotypic abnormality (HP:0000118)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0000001",
        `name` = "All",
        `parents` = Nil,
        `age_at_event` = Set(56, 63)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0025031",
        `name` = "Abnormality of the digestive system",
        `parents` = Seq("Phenotypic abnormality (HP:0000118)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0012718",
        `name` = "Morphological abnormality of the gastrointestinal tract",
        `parents` = Seq("Abnormality of the gastrointestinal tract (HP:0011024)", "Abnormality of digestive system morphology (HP:0025033)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0004386",
        `name` = "Gastrointestinal inflammation",
        `parents` = Seq("Increased inflammatory response (HP:0012649)", "Functional abnormality of the gastrointestinal tract (HP:0012719)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0012649",
        `name` = "Increased inflammatory response",
        `parents` = Seq("Abnormal inflammatory response (HP:0012647)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0010978",
        `name` = "Abnormality of immune system physiology",
        `parents` = Seq("Abnormality of the immune system (HP:0002715)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0012719",
        `name` = "Functional abnormality of the gastrointestinal tract",
        `parents` = Seq("Abnormality of the gastrointestinal tract (HP:0011024)", "Abnormality of digestive system physiology (HP:0025032)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0002250",
        `name` = "Abnormal large intestine morphology",
        `parents` = Seq("Abnormal intestine morphology (HP:0002242)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0012647",
        `name` = "Abnormal inflammatory response",
        `parents` = Seq("Abnormality of immune system physiology (HP:0010978)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0100281",
        `name` = "Chronic colitis",
        `parents` = Seq("Colitis (HP:0002583)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0002242",
        `name` = "Abnormal intestine morphology",
        `parents` = Seq("Morphological abnormality of the gastrointestinal tract (HP:0012718)"),
        `age_at_event` = Set(56)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0025033",
        `name` = "Abnormality of digestive system morphology",
        `parents` = Seq("Abnormality of the digestive system (HP:0025031)"),
        `age_at_event` = Set(56)
      )
    )
  }

  "Donors" should "map non observed hpo terms per donors" in {
    import spark.implicits._

    val phenotypesForDonor14 = df.filter($"study_id" === "ST0001" && $"submitter_donor_id" === "PT00014").select(col = "non_observed_phenotypes").as[Seq[ONTOLOGY_TERM]].collect().head

    val phenotypesForDonor14TaggedDf = df.filter($"submitter_donor_id" === "PT00014")
      .select($"not_observed_phenotype_tagged")
    val phenotypesForDonor14Tagged = phenotypesForDonor14TaggedDf.as[Seq[ONTOLOGY_TERM]].collect().head

    phenotypesForDonor14Tagged should contain theSameElementsAs Seq(
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0025031",
        `name` = "Abnormality of the digestive system",
        `parents` = Seq("Phenotypic abnormality (HP:0000118)"),
        `age_at_event` = Set(54),
        `is_leaf` = false,
        `is_tagged` = true
      )
    )

    phenotypesForDonor14 should contain theSameElementsAs Seq(
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0025031",
        `name` = "Abnormality of the digestive system",
        `parents` = Seq("Phenotypic abnormality (HP:0000118)"),
        `age_at_event` = Set(54),
        `is_leaf` = false,
        `is_tagged` = true
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0000118",
        `name` = "Phenotypic abnormality",
        `parents` = Seq("All (HP:0000001)"),
        `age_at_event` = Set(54)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "HP:0000001",
        `name` = "All",
        `parents` = Nil,
        `age_at_event` = Set(54)
      )
    )
  }

  "Donors" should "map mondo terms" in {
    import spark.implicits._

    val mondoDiagnosisForDonor14 = df.filter($"study_id" === "ST0001" && $"submitter_donor_id" === "PT00014").select(col = "mondo").as[Seq[ONTOLOGY_TERM]].collect().head
    val mondoDiagnosisForDonor14TaggedDf = df.filter($"study_id" === "ST0001" && $"submitter_donor_id" === "PT00014").select(col = "diagnoses.tagged_mondo")
    val mondoDiagnosisForDonor14Tagged = mondoDiagnosisForDonor14TaggedDf.as[Seq[ONTOLOGY_TERM]].collect().head
    val main_category14 = mondoDiagnosisForDonor14TaggedDf.select($"tagged_mondo.main_category")

    main_category14.as[Seq[String]].collect().head should contain theSameElementsAs Seq("disease by anatomical system (MONDO:0021199)")

    mondoDiagnosisForDonor14Tagged should contain theSameElementsAs Seq(
      ONTOLOGY_TERM(
        `phenotype_id` = "MONDO:0005041",
        `name` = "glaucoma (disease)",
        `parents` = Seq("eye disease (MONDO:0005328)"),
        `age_at_event` = Set(59),
        `is_leaf` = false,
        `is_tagged` = true
      )
    )

    mondoDiagnosisForDonor14 should contain theSameElementsAs Seq(
      ONTOLOGY_TERM(
        `phenotype_id` = "MONDO:0021199",
        `name` = "disease by anatomical system",
        `parents` = Seq("disease or disorder (MONDO:0000001)"),
        `age_at_event` = Set(59)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "MONDO:0024458",
        `name` = "disease of visual system",
        `parents` = Seq("disease by anatomical system (MONDO:0021199)"),
        `age_at_event` = Set(59)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "MONDO:0005328",
        `name` = "eye disease",
        `parents` = Seq("disease of orbital region (MONDO:0002022)", "disease of visual system (MONDO:0024458)"),
        `age_at_event` = Set(59)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "MONDO:0024505",
        `name` = "disorder by anatomical region",
        `parents` = Seq("disease or disorder (MONDO:0000001)"),
        `age_at_event` = Set(59)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "MONDO:0021059",
        `name` = "head or neck disease/disorder",
        `parents` = Seq("disorder by anatomical region (MONDO:0024505)"),
        `age_at_event` = Set(59)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "MONDO:0044987",
        `name` = "face disease",
        `parents` = Seq("head disease (MONDO:0005042)"),
        `age_at_event` = Set(59)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "MONDO:0002022",
        `name` = "disease of orbital region",
        `parents` = Seq("face disease (MONDO:0044987)"),
        `age_at_event` = Set(59)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "MONDO:0005042",
        `name` = "head disease",
        `parents` = Seq("head or neck disease/disorder (MONDO:0021059)"),
        `age_at_event` = Set(59)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "MONDO:0005041",
        `name` = "glaucoma (disease)",
        `parents` = Seq("eye disease (MONDO:0005328)"),
        `age_at_event` = Set(59),
        `is_leaf` = false,
        `is_tagged` = true,
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "MONDO:0000001",
        `name` = "disease or disorder",
        `parents` = Nil,
        `age_at_event` = Set(59)
      )
    )
  }

  "Donors" should "map icd terms" in {
    import spark.implicits._

    val icdDiagnosisForDonor14TaggedDf = df.filter($"study_id" === "ST0001" && $"submitter_donor_id" === "PT00014").select(col = "diagnoses.tagged_icd")
    val icdDiagnosisForDonor14 = df.filter($"study_id" === "ST0001" && $"submitter_donor_id" === "PT00014").select(col = "icd").as[Seq[ONTOLOGY_TERM]].collect().head
    val icdDiagnosisForDonor14Tagged = icdDiagnosisForDonor14TaggedDf.as[Seq[ONTOLOGY_TERM]].collect().head
    val main_category14 = icdDiagnosisForDonor14TaggedDf.select($"tagged_icd.main_category")

    main_category14.as[Seq[String]].collect().head should contain theSameElementsAs Seq("Glaucoma (H40-H42)")

    icdDiagnosisForDonor14Tagged should contain theSameElementsAs Seq(
      ONTOLOGY_TERM(
        `phenotype_id` = "H40",
        `name` = "Glaucoma",
        `parents` = Seq("Glaucoma (H40-H42)"),
        `age_at_event` = Set(59),
        `is_leaf` = false,
        `is_tagged` = true
      )
    )

    icdDiagnosisForDonor14 should contain theSameElementsAs Seq(
      ONTOLOGY_TERM(
        `phenotype_id` = "H40",
        `name` = "Glaucoma",
        `parents` = Seq("Glaucoma (H40-H42)"),
        `age_at_event` = Set(59),
        `is_leaf` = false,
        `is_tagged` = true
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "H40-H42",
        `name` = "Glaucoma",
        `parents` = Nil,
        `age_at_event` = Set(59)
      ),
      ONTOLOGY_TERM(
        `phenotype_id` = "",
        `name` = "Diseases of the eye and adnexa",
        `parents` = Nil,
        `age_at_event` = Set(59),
      ),
    )
  }
}

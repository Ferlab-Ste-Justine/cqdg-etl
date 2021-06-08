package ca.cqdg.etl

import ca.cqdg.etl.model.{NamedDataFrame, S3File}
import ca.cqdg.etl.utils.EtlUtils.columns.notNullCol
import ca.cqdg.etl.utils.EtlUtils.{getConfiguration, getDataframe, loadAll}
import ca.cqdg.etl.utils.PreProcessingUtils.{getOntologyDfs, loadSchemas, preProcess}
import ca.cqdg.etl.utils.S3Utils.writeSuccessIndicator
import ca.cqdg.etl.utils.{S3Utils, Schema}
import com.amazonaws.ClientConfiguration
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object EtlApp extends App {

  Logger.getLogger("ferlab").setLevel(Level.INFO)

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("CQDG-ETL")
    .master("local")//TODO: Remove "local" and replace by '[*]'
    .getOrCreate()

  // https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#general_configuration
  // The following env var must be set :
  // SERVICE_ENDPOINT
  // AWS_ACCESS_KEY_ID
  // AWS_SECRET_ACCESS_KEY
  // AWS_DEFAULT_REGION
  val s3Endpoint = getConfiguration("SERVICE_ENDPOINT", "http://localhost:9000")
  val s3Bucket: String = getConfiguration("AWS_BUCKET", "cqdg")

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3Endpoint)
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")

  val clientConfiguration = new ClientConfiguration
  clientConfiguration.setSignerOverride("AWSS3V4SignerType")

  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withEndpointConfiguration(
      new EndpointConfiguration(
        getConfiguration("SERVICE_ENDPOINT", s3Endpoint),
        getConfiguration("AWS_DEFAULT_REGION", Regions.US_EAST_1.name()))
    )
    .withPathStyleAccessEnabled(true)
    .withClientConfiguration(clientConfiguration)
    .build()

  val filesPerFolder: Map[String, List[S3File]] = S3Utils.loadFileEntries(s3Bucket, "clinical-data", s3Client)

  val ontologyFiles = S3Utils.loadFileEntry(s3Bucket, "ontology-input", s3Client)
  val ontologyDfs = getOntologyDfs(ontologyFiles)

  val dictionarySchemas: Map[String, List[Schema]] = loadSchemas()
  val readyToProcess: Map[String, List[NamedDataFrame]] = preProcess(filesPerFolder, s3Bucket)(dictionarySchemas);

  import spark.implicits._
  readyToProcess.foreach { case (prefix, dfList) =>
    val outputPath= s"s3a://${s3Bucket}/clinical-data-etl-indexer"

    val studyNDF = getDataframe("study", dfList)
      val study: DataFrame = studyNDF.dataFrame
        .select(
          $"*",
          $"study_id" as "study_id_keyword",
          $"short_name" as "short_name_keyword",
        )
        .withColumn("short_name", notNullCol($"short_name"))
        .as("study")

      val (dataAccess, donor, diagnosisPerDonorAndStudy, phenotypesPerStudyIdAndDonor, biospecimenWithSamples, file, treatmentsPerDonorAndStudy, exposuresPerDonorAndStudy, followUpsPerDonorAndStudy, familyHistoryPerDonorAndStudy, familyRelationshipPerDonorAndStudy) = loadAll(dfList)(ontologyDfs)

      val inputData = Map(
        "donor" -> donor,
        "diagnosisPerDonorAndStudy" -> diagnosisPerDonorAndStudy,
        "phenotypesPerStudyIdAndDonor" -> phenotypesPerStudyIdAndDonor,
        "biospecimenWithSamples" -> biospecimenWithSamples,
        "dataAccess" -> dataAccess,
        "treatmentsPerDonorAndStudy" -> treatmentsPerDonorAndStudy,
        "exposuresPerDonorAndStudy" -> exposuresPerDonorAndStudy,
        "followUpsPerDonorAndStudy" -> followUpsPerDonorAndStudy,
        "familyHistoryPerDonorAndStudy" -> familyHistoryPerDonorAndStudy,
        "familyRelationshipPerDonorAndStudy" -> familyRelationshipPerDonorAndStudy,
        "file" -> file)

      Donor.run(study, studyNDF, inputData, ontologyDfs, s"$outputPath/donors" )
      Study.run(study, studyNDF, inputData, ontologyDfs, s"$outputPath/studies")

      val files = new FileIndex(study, studyNDF, inputData)(etlConfiguration);
      write(files.transform(files.extract()), s"$outputPath/files")

      writeSuccessIndicator(s3Bucket, prefix, s3Client);
  }

  def write(files: DataFrame, outputPath: String)(implicit spark: SparkSession): Unit = {
    files
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("study_id", "dictionary_version", "study_version", "study_version_creation_date")
      .json(outputPath)
  }

  spark.stop()
}

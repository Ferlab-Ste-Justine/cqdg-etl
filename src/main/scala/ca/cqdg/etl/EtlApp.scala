package ca.cqdg.etl

import ca.cqdg.etl.model.{NamedDataFrame, S3File}
import ca.cqdg.etl.utils.EtlUtils.columns.notNullCol
import ca.cqdg.etl.utils.EtlUtils.{getConfiguration, getDataframe, loadAll}
import ca.cqdg.etl.utils.PreProcessingUtils.{getOntologyDfs, loadSchemas, preProcess}
import ca.cqdg.etl.utils.S3Utils.writeSuccessIndicator
import ca.cqdg.etl.utils.{DataAccessUtils, KeycloakUtils, S3Utils, Schema}
import com.amazonaws.ClientConfiguration
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService}

object EtlApp extends App {

  val log = LoggerFactory.getLogger(this.getClass)

  val filesPerFolder: Map[String, List[S3File]] = S3Utils.loadFileEntries(s3Bucket, "clinical-data", s3Client)

  val dictionarySchemas: Map[String, List[Schema]] = loadSchemas()
  val readyToProcess: Map[String, List[NamedDataFrame]] = preProcess(filesPerFolder, s3Bucket)(dictionarySchemas);

  import spark.implicits._

  val ontologyFiles = S3Utils.loadFileEntry(s3Bucket, "ontology-input", s3Client)
  val ontologyDfs = getOntologyDfs(ontologyFiles)

  readyToProcess.foreach { case (prefix, dfList) =>
    val outputPath= s"s3a://${s3Bucket}/clinical-data-etl-indexer"

    val studyNDF = getDataframe("study", dfList)
    
    val (donor, diagnosisPerDonorAndStudy, phenotypesPerStudyIdAndDonor, biospecimenWithSamples, file, treatmentsPerDonorAndStudy, exposuresPerDonorAndStudy, followUpsPerDonorAndStudy, familyHistoryPerDonorAndStudy, familyRelationshipPerDonorAndStudy) = loadAll(dfList)(ontologyDfs)

    val dataAccessGroup = DataAccessUtils.computeDataAccessByEntityType(studyNDF.dataFrame, ontologyDfs("duo_code"))

    val study: DataFrame = studyNDF.dataFrame
      .join(dataAccessGroup, Seq("study_id"), "left")
        .select(
          $"*",
          $"study_id" as "study_id_keyword",
          $"short_name" as "short_name_keyword",
        )
        .drop("access_limitations", "access_requirements")
        .withColumn("short_name", notNullCol($"short_name"))
        .as("study")
    
      val inputData = Map(
        "donor" -> donor,
        "diagnosisPerDonorAndStudy" -> diagnosisPerDonorAndStudy,
        "phenotypesPerStudyIdAndDonor" -> phenotypesPerStudyIdAndDonor,
        "biospecimenWithSamples" -> biospecimenWithSamples,
        "treatmentsPerDonorAndStudy" -> treatmentsPerDonorAndStudy,
        "exposuresPerDonorAndStudy" -> exposuresPerDonorAndStudy,
        "followUpsPerDonorAndStudy" -> followUpsPerDonorAndStudy,
        "familyHistoryPerDonorAndStudy" -> familyHistoryPerDonorAndStudy,
        "familyRelationshipPerDonorAndStudy" -> familyRelationshipPerDonorAndStudy,
        "file" -> file)

      Donor.run(study, studyNDF, inputData, ontologyDfs("duo_code"), s"$outputPath/donors" )
      Study.run(study, studyNDF, inputData, ontologyDfs, s"$outputPath/studies")

      val files = new FileIndex(study, studyNDF, inputData, ontologyDfs("duo_code"))(etlConfiguration);
      val transformedFiles = files.transform(files.extract())

      if(KeycloakUtils.isEnabled) {
        val allFilesInternalIDs = transformedFiles.select("internal_file_id").distinct().as[String].collect().toSet
        val future = KeycloakUtils.createResources(allFilesInternalIDs)
        val resources = Await.result(future, Duration.Inf)
        log.info(s"Successfully create ${resources.size} resources")
      }

      write(transformedFiles, s"$outputPath/files")

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

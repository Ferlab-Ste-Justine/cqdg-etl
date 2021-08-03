package ca.cqdg.etl

import ca.cqdg.etl.PreProcess.{log, preProcessInput}
import ca.cqdg.etl.model.{NamedDataFrame, S3File}
import ca.cqdg.etl.utils.EtlUtils.columns.notNullCol
import ca.cqdg.etl.utils.EtlUtils.{getDataframe, loadAll}
import ca.cqdg.etl.utils.PreProcessingUtils.getOntologyDfs
import ca.cqdg.etl.utils.S3Utils.writeSuccessIndicator
import ca.cqdg.etl.utils.{DataAccessUtils, KeycloakUtils, S3Utils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Process extends App {

  import spark.implicits._

  val preProcessConfig: Config = ConfigFactory.load.getObject("pre-process").toConfig

  val processConfig: Config = ConfigFactory.load.getObject("process").toConfig
  val processLogger = processConfig.getString("logger")
  val processInput = processConfig.getString("input")
  val processOutput = processConfig.getString("output")

  val log = LoggerFactory.getLogger(processLogger)
  Logger.getLogger(processLogger).setLevel(Level.INFO)

  def buildNamedDataFrame(s3Bucket: String, files: Map[String, List[String]]): Map[String, List[NamedDataFrame]] = {
     val ndfsByPath = files.map({case (path, files) =>
        val dfs = files.map(file => {
          val s3File = s"s3a://$s3Bucket//$file"
          val dfName = file.substring(file.lastIndexOf("/") + 1)
          val df = preProcessConfig.getString("format") match {
            case "tsv" => spark.read.option("header", "true").option("delimiter", "\t").csv(s"$s3File/*.csv")
            case "parquet" => spark.read.parquet(s3File)
            case format => throw new RuntimeException(s"Unsupported pre-process format: $format")
          }
          if(dfName == "study") {
            val metadatas = df.select("study_version","study_version_creation_date", "dictionary_version").distinct().first()
            NamedDataFrame(dfName, df, metadatas.getString(0), metadatas.getString(1), metadatas.getString(2))
          }else {
            NamedDataFrame(dfName, df, null, null, null)
          }
        })
       (path, dfs)
     })
    ndfsByPath.map({case (path, ndfs) =>
      val studyNdf = ndfs.find(ndf => ndf.name.equals("study")).getOrElse(throw new RuntimeException("Can't find study NamedDataFrame in: " + path))
      ndfs.foreach(otherNdf => {
        if(!otherNdf.name.equals("study")) {
          otherNdf.studyVersion = studyNdf.studyVersion
          otherNdf.studyVersionCreationDate = studyNdf.studyVersionCreationDate
          otherNdf.dictionaryVersion = studyNdf.dictionaryVersion
        }
      })
      (path, ndfs)
    })
  }

  def run():Unit = {
    log.info("Looking for un-processed S3 files ...")
    val filesPerFolder: Map[String, List[String]] = S3Utils.loadPreProcessedEntries(s3Bucket, processInput, s3Client)
    log.info("Preparing NamedDataFrames ... ...")
    val readyToProcess: Map[String, List[NamedDataFrame]] = buildNamedDataFrame(s3Bucket, filesPerFolder)
    log.info("Loading ontology files ...")
    val ontologyFiles = S3Utils.loadFileEntry(s3Bucket, "ontology-input", s3Client)
    log.info("Extracting ontology files ...")
    val ontologyDfs = getOntologyDfs(ontologyFiles)

    readyToProcess.foreach { case (prefix, dfList) =>
      val outputPath= s"s3a://${s3Bucket}/$processOutput"

      val studyNDF = getDataframe("study", dfList)

      log.info("Computing PerDonorAndStudy ...")
      val (donor, diagnosisPerDonorAndStudy, phenotypesPerStudyIdAndDonor, biospecimenWithSamples, file, treatmentsPerDonorAndStudy, exposuresPerDonorAndStudy, followUpsPerDonorAndStudy, familyHistoryPerDonorAndStudy, familyRelationshipPerDonorAndStudy) = loadAll(dfList)(ontologyDfs)

      log.info("Computing DataAccessGroup ...")
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

      log.info("Computing Donor ...")
      Donor.run(study, studyNDF, inputData, ontologyDfs("duo_code"), s"$outputPath/donors" )
      log.info("Computing Study ...")
      Study.run(study, studyNDF, inputData, ontologyDfs, s"$outputPath/studies")
      log.info("Computing File ...")
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
  }

  def write(files: DataFrame, outputPath: String): Unit = {
    files
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("study_id", "dictionary_version", "study_version", "study_version_creation_date")
      .json(outputPath)
  }

  run()

  spark.stop()
}

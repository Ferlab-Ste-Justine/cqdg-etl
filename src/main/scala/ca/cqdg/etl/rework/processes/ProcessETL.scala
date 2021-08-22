package ca.cqdg.etl.rework.processes

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import ca.cqdg.etl.rework.EtlUtils.sanitize
import ca.cqdg.etl.rework.models.{Metadata, NamedDataFrame}
import ca.cqdg.etl.rework.processes.ProcessETLUtils.columns.notNullCol
import ca.cqdg.etl.rework.processes.ProcessETLUtils.{computeDataAccessByEntityType, loadAll}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

class ProcessETL()(implicit spark: SparkSession, conf: Configuration) {

  val log: slf4j.Logger = LoggerFactory.getLogger("process")
  Logger.getLogger("process").setLevel(Level.INFO)

  val biospecimen: DatasetConf = conf.getDataset("biospecimen")
  val diagnosis: DatasetConf = conf.getDataset("diagnosis")
  val donor: DatasetConf = conf.getDataset("donor")
  val exposure: DatasetConf = conf.getDataset("exposure")
  val family_history: DatasetConf = conf.getDataset("family_history")
  val family: DatasetConf = conf.getDataset("family")
  val file: DatasetConf = conf.getDataset("file")
  val follow_up: DatasetConf = conf.getDataset("follow_up")
  val phenotype: DatasetConf = conf.getDataset("phenotype")
  val sample_registration: DatasetConf = conf.getDataset("sample_registration")
  val study: DatasetConf = conf.getDataset("study")
  val treatment: DatasetConf = conf.getDataset("treatment")
  val hpo: DatasetConf = conf.getDataset("hpo")
  val mondo: DatasetConf = conf.getDataset("mondo")
  val icd: DatasetConf = conf.getDataset("icd")
  val duo_code: DatasetConf = conf.getDataset("duo_code")

  def extract(): Map[String, DataFrame] = {
    log.info("Extract ETL inputs ...")
    Map(
      biospecimen.id -> biospecimen.read,
      diagnosis.id -> diagnosis.read,
      donor.id -> donor.read,
      exposure.id -> exposure.read,
      family_history.id -> family_history.read,
      family.id -> family.read,
      file.id -> file.read,
      follow_up.id -> follow_up.read,
      phenotype.id -> phenotype.read,
      sample_registration.id -> sample_registration.read,
      study.id -> study.read,
      treatment.id -> treatment.read,
      hpo.id -> hpo.read,
      mondo.id -> mondo.read,
      icd.id -> icd.read,
      duo_code.id -> duo_code.read,
    )
  }

  def transform(data: Map[String, DataFrame]): Unit = {

    import spark.implicits._

    log.info("Extract metadata ...")
    val metadata = extractMetadata(data(study.id))
    log.info("Build named dataframes ...")
    val ndfs = buildNamedDataFrames(data, metadata)
    val dfList = ndfs.values.toList

    val ontologyDfs = Map(
      hpo.id -> data(hpo.id),
      mondo.id -> data(mondo.id),
      icd.id -> data(icd.id),
      duo_code.id -> data(duo_code.id),
    )

    val studyNDF = ndfs(study.id)

    log.info("Computing PerDonorAndStudy ...")
    val (donor, diagnosisPerDonorAndStudy, phenotypesPerStudyIdAndDonor, biospecimenWithSamples, file, treatmentsPerDonorAndStudy, exposuresPerDonorAndStudy, followUpsPerDonorAndStudy, familyHistoryPerDonorAndStudy, familyRelationshipPerDonorAndStudy) = loadAll(dfList)(ontologyDfs)

    log.info("Computing DataAccessGroup ...")
    val dataAccessGroup = computeDataAccessByEntityType(studyNDF.dataFrame, ontologyDfs(duo_code.id))

    val studyDf: DataFrame = studyNDF.dataFrame
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

    log.info("Computing Study ...")

    log.info("Computing Donor ...")

    log.info("Computing File ...")
    val files = new FileIndex(studyDf, metadata, inputData)(conf);
    val transformedFiles = files.transform(files.extract())
    write("files", transformedFiles)

  }

  private def write(name: String, df: DataFrame): Unit = {
    val source = conf.getDataset(name)
    val storage = conf.getStorage(source.storageid)
    val outputPath = s"$storage/${source.path}"

    df
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy(source.partitionby:_*)
      .json(outputPath)
  }

  private def extractMetadata(dfStudy: DataFrame): Metadata = {
    val data = dfStudy.select("study_version", "study_version_creation_date", "dictionary_version").distinct().first()
    Metadata(data.getString(0), data.getString(1), data.getString(2))
  }

  private def buildNamedDataFrames(data: Map[String, DataFrame], metadata: Metadata): Map[String, NamedDataFrame] = {
    data.map({ case (name, df) =>
      (name, NamedDataFrame(sanitize(name), df, metadata.studyVersion, metadata.studyVersionCreationDate, metadata.dictionaryVersion))
    })
  }
}

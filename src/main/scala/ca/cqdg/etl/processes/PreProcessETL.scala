package ca.cqdg.etl.processes

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import ca.cqdg.etl.clients.inf.{IDictionary, IIdServer}
import ca.cqdg.etl.EtlUtils.sanitize
import PreProcessUtils.addCQDGId
import ca.cqdg.etl.models.{Metadata, NamedDataFrame, Schema}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

class PreProcessETL(dictionaryClient: IDictionary, idServerClient: IIdServer)(implicit spark: SparkSession, conf: Configuration) {

  val log: slf4j.Logger = LoggerFactory.getLogger("pre-process")
  Logger.getLogger("pre-process").setLevel(Level.INFO)

  val study_version_metadata: DatasetConf = conf.getDataset("study_version_metadata")
  val biospecimen: DatasetConf = conf.getDataset("biospecimen")
  val diagnosis: DatasetConf = conf.getDataset("diagnosis")
  val donor: DatasetConf = conf.getDataset("donor")
  val exposure: DatasetConf = conf.getDataset("exposure")
  val family_history: DatasetConf = conf.getDataset("family-history")
  val family: DatasetConf = conf.getDataset("family")
  val file: DatasetConf = conf.getDataset("file")
  val follow_up: DatasetConf = conf.getDataset("follow-up")
  val phenotype: DatasetConf = conf.getDataset("phenotype")
  val sample_registration: DatasetConf = conf.getDataset("sample_registration")
  val study: DatasetConf = conf.getDataset("study")
  val treatment: DatasetConf = conf.getDataset("treatment")

  def extract(): Map[String, DataFrame] = {
    log.info("Extract ETL inputs ...")
    Map(
      study_version_metadata.id -> study_version_metadata.read,
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
    )
  }

  private def extractMetadata(dfMetadata: DataFrame): Metadata = {
    val data = dfMetadata.select("studyVersionId", "studyVersionDate", "dictionaryVersion").distinct().first()
    Metadata(data.getString(0), data.getString(1), data.getString(2))
  }

  def transform(data: Map[String, DataFrame]): List[NamedDataFrame] = {
    log.info("Retrieve dictionary schemas ...")
    val dictionarySchemas: Map[String, List[Schema]] = dictionaryClient.loadSchemas()

    log.info("Extract meta-datas ...")
    val metadata = extractMetadata(data(study_version_metadata.id))

    log.info("Find matching CQDG schema ...")
    val schemaEntities: List[Schema] = dictionarySchemas.getOrElse(metadata.dictionaryVersion, throw new RuntimeException(s"Failed to load dictionary schema for version ${metadata.dictionaryVersion}"))

    data
      .filter({ case (name, _) => schemaEntities.map(s => s.name).contains(sanitize(name)) })
      .map({
        case (name, df) =>
          val sanitizedName = sanitize(name)
          val cqdgIDsAdded: DataFrame = addCQDGId(sanitizedName, df, idServerClient.getCQDGIds)

          // Remove columns that are not in the schema
          val colsToRemove = cqdgIDsAdded.columns.filterNot(col => schemaEntities.find(schema => schema.name == sanitizedName).get.columns.contains(col))
          if (colsToRemove.length > 0) {
            log.warn(s"Removing the columns [${colsToRemove.mkString(",")}] from ${name}")
          }

          var sanitizedDF: DataFrame = cqdgIDsAdded.drop(colsToRemove: _*)

          if (name.equals("study")) { // TODO all of them ?
            log.info("Add meta-data columns to study")
            sanitizedDF = sanitizedDF.withColumn("dictionary_version", lit(metadata.dictionaryVersion))
              .withColumn("study_version", lit(metadata.studyVersion))
              .withColumn("study_version_creation_date", lit(metadata.studyVersionCreationDate))
          }

          NamedDataFrame(name, sanitizedDF, metadata.studyVersion, metadata.studyVersionCreationDate, metadata.dictionaryVersion)
      }).toList
  }


  def load(ndfs: List[NamedDataFrame]): Unit = {
    ndfs.foreach(ndf => {
      log.info(s"Save ${ndf.name} ...")
      write(s"${ndf.name}-with-ids", ndf.dataFrame, conf)
    })
  }

}

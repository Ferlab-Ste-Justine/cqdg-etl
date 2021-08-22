package ca.cqdg.etl.rework.processes

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import ca.cqdg.etl.rework.EtlUtils.sanitize
import ca.cqdg.etl.rework.clients.inf.{IDictionary, IIdServer}
import ca.cqdg.etl.rework.models.{Metadata, NamedDataFrame, Schema}
import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, concat_ws, lit, sha1}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters

class PreProcessETL(dictionaryClient: IDictionary, idServerClient: IIdServer)(implicit spark: SparkSession, conf: Configuration) {

  val gson: Gson = new Gson()

  val log: slf4j.Logger = LoggerFactory.getLogger("pre-process")
  Logger.getLogger("pre-process").setLevel(Level.INFO)

  val study_version_metadata: DatasetConf = conf.getDataset("study_version_metadata")
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
    val data = dfMetadata.select("dictionaryVersion", "studyVersionId", "studyVersionDate").distinct().first()
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
      .map({ case (name, df) => sanitize(name) -> df }) // sanitize name
      .filter({ case (name, _) => schemaEntities.map(s => s.name).contains(name) })
      .map({
        case (name, df) =>
          val cqdgIDsAdded: DataFrame = addCQDGId(name, df)

          // Remove columns that are not in the schema
          val colsToRemove = cqdgIDsAdded.columns.filterNot(col => schemaEntities.find(schema => schema.name == name).get.columns.contains(col))
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

  private def addCQDGId(name: String, df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val (enhancedDF: DataFrame, entityType: String) = sanitize(name) match {
      case "familyhistory" => (df
        .withColumn("cqdg_entity", lit("family_history"))
        .withColumn(
          "cqdg_hash",
          sha1(concat_ws("_", lit("family_history"), col("study_id"), col("submitter_donor_id"), col("submitter_family_condition_id"))))
        , "family_history")
      case "diagnosis" => (df
        .withColumn("cqdg_entity", lit("diagnosis"))
        .withColumn(
          "cqdg_hash",
          sha1(concat_ws("_", lit("diagnosis"), col("study_id"), col("submitter_donor_id"), col("submitter_diagnosis_id")))),
        "diagnosis")
      case "donor" => (df
        .withColumn("cqdg_entity", lit("donor"))
        .withColumn(
          "cqdg_hash",
          sha1(concat_ws("_", lit("donor"), col("study_id"), col("submitter_donor_id")))),
        "donor")
      case "treatment" => (df
        .withColumn("cqdg_entity", lit("treatment"))
        .withColumn(
          "cqdg_hash",
          sha1(concat_ws("_", lit("treatment"), col("study_id"), col("submitter_donor_id"), col("submitter_treatment_id")))),
        "treatment")
      case "family" => (df
        .withColumn("cqdg_entity", lit("family"))
        .withColumn(
          "cqdg_hash",
          sha1(concat_ws("_", lit("family"), col("study_id"), col("submitter_family_id"), col("submitter_donor_id")))),
        "family")
      case "biospecimen" => (df
        .withColumn("cqdg_entity", lit("biospecimen"))
        .withColumn(
          "cqdg_hash",
          sha1(concat_ws("_", lit("biospecimen"), col("study_id"), col("submitter_donor_id"), col("submitter_biospecimen_id")))),
        "biospecimen")
      case "exposure" => (df
        .withColumn("cqdg_entity", lit("exposure"))
        .withColumn(
          "cqdg_hash",
          sha1(concat_ws("_", lit("exposure"), col("study_id"), col("submitter_donor_id")))),
        "exposure")
      case "sampleregistration" => (df
        .withColumn("cqdg_entity", lit("sample_registration"))
        .withColumn(
          "cqdg_hash",
          sha1(concat_ws("_", lit("sample_registration"), col("study_id"), col("submitter_donor_id"), col("submitter_biospecimen_id"), col("submitter_sample_id")))),
        "sample_registration")
      case "followup" => (df
        .withColumn("cqdg_entity", lit("follow_up"))
        .withColumn(
          "cqdg_hash",
          sha1(concat_ws("_", lit("follow_up"), col("study_id"), col("submitter_donor_id"), col("submitter_follow_up_id")))),
        "follow_up")
      case "phenotype" => (df
        .withColumn("cqdg_entity", lit("phenotype"))
        .withColumn(
          "cqdg_hash",
          sha1(concat_ws("_", lit("phenotype"), col("study_id"), col("submitter_donor_id"), col("submitter_phenotype_id"), col("phenotype_HPO_code")))),
        "phenotype")
      case "study" => (df
        .withColumn("cqdg_entity", lit("study"))
        .withColumn(
          "cqdg_hash",
          sha1(concat_ws("_", lit("study"), col("study_id")))),
        "study")
      case "file" => (df
        .withColumn("cqdg_entity", lit("file"))
        .withColumn(
          "cqdg_hash",
          sha1(concat_ws("_", lit("file"), col("study_id"), col("submitter_donor_id"), col("file_name")))),
        "file")
      case _ => throw new RuntimeException(s"Could not find the corresponding schema to the given file ${name}")
    }

    val result = if (enhancedDF.columns.contains("cqdg_hash")) {
      val idServicePayload = enhancedDF.select("cqdg_hash", "cqdg_entity").as[(String, String)].collect().toMap
      val jsonResponse = idServerClient.getCQDGIds(gson.toJson(JavaConverters.mapAsJavaMap(idServicePayload)))
      val cqdgIDsDF = spark.read.json(Seq(jsonResponse).toDS()).toDF("hash", "internal_id")
      enhancedDF
        .join(cqdgIDsDF, $"cqdg_hash" === $"hash")
        .drop("cqdg_hash", "hash")
        .withColumnRenamed("internal_id", s"internal_${sanitize(entityType)}_id")
    } else {
      enhancedDF
    }
    result
  }

  def load(ndfs: List[NamedDataFrame]): Unit = {
    // work-around to use the config = create an anonymous ETL object that load 1 dataframe
    ndfs.foreach(ndf => {
      val etl = new ETL() {
        override val destination: DatasetConf = conf.getDataset(s"${ndf.name}-with-ids")

        override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = Map(ndf.name -> ndf.dataFrame)

        override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = ndf.dataFrame
      }
      log.info(s"Save ${ndf.name} ...")
      etl.load(etl.transform(etl.extract()))
    })
  }

}

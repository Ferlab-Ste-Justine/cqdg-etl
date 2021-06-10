package ca.cqdg.etl.utils

import ca.cqdg.etl.model
import ca.cqdg.etl.model.{NamedDataFrame, S3File}
import ca.cqdg.etl.utils.EtlUtils.{getConfiguration, sanitize}
import ca.cqdg.etl.utils.ExternalApi.getCQDGIds
import com.google.gson.{Gson, JsonArray, JsonParser}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import sttp.client3.{HttpURLConnectionBackend, basicRequest, _}
import sttp.model.StatusCode

import scala.collection.{JavaConverters, mutable}

object PreProcessingUtils{

  val dictionaryUsername: String = getConfiguration("LECTERN_USERNAME", "lectern")
  val dictionaryPassword: String = getConfiguration("LECTERN_PASSWORD", "changeMe")
  val dictionaryName: String = getConfiguration("LECTERN_DICTIONARY_NAME", "CQDG Data Dictionary")
  val dictionaryURL: String = getConfiguration("LECTERN_HOST", "https://schema.qa.cqdg.ferlab.bio")
  val idServiceURL: String = getConfiguration("ID_SERVICE_HOST", "http://localhost:5000")
  val gson: Gson = new Gson()
  val LOG: Logger = Logger.getLogger(this.getClass)

  def preProcess(files: Map[String, List[S3File]], s3Bucket: String, buildIds: String => String = getCQDGIds)
                (dictionarySchemas: Map[String, List[Schema]])
                (implicit spark: SparkSession): Map[String, List[NamedDataFrame]] = {
    files.map({
      case(key, values) =>
        key -> preProcess(values, s3Bucket: String)(dictionarySchemas, buildIds)
    })
  }

  def preProcess(files: List[S3File], s3Bucket: String)
                (dictionarySchemas: Map[String, List[Schema]], buildIds: String => String)
                (implicit spark: SparkSession): List[NamedDataFrame] = {
    val metadata = files
                      .find(f => f.filename == "study_version_metadata.json")
                      .getOrElse(throw new RuntimeException("study_version_metadata.json file not present. Cannot proceed."))
    val metadataDF: DataFrame = spark.read.option("multiline", "true").json(s"s3a://${s3Bucket}/${metadata.key}")
    val dictionaryVersion: String = metadataDF.select("dictionaryVersion").collectAsList().get(0).getString(0)
    val studyVersion: String = metadataDF.select("studyVersionId").collectAsList().get(0).getString(0)
    val studyVersionCreationDate: String = metadataDF.select("studyVersionDate").collectAsList().get(0).getString(0)
    val schemaEntities: List[Schema] = dictionarySchemas.getOrElse(dictionaryVersion, throw new RuntimeException(s"Failed to load dictionary schema for version ${dictionaryVersion}"))

    files
      // Filter out all files that are not part of the dictionary version for the current study
      .filter(f => schemaEntities.map(s => s.name).contains(f.schema))
      .map(f = f => {
        val cqdgIDsAdded: DataFrame = addCQDGId(f, buildIds)

        // Remove columns that are not in the schema
        val colsToRemove = cqdgIDsAdded.columns.filterNot(col => schemaEntities.find(schema => schema.name == f.schema).get.columns.contains(col))
        if(colsToRemove.length > 0){
          LOG.warn(s"Removing the columns [${colsToRemove.mkString(",")}] from ${f.filename}")
        }

        val sanitizedDF: DataFrame = cqdgIDsAdded.drop(colsToRemove:_*)
        model.NamedDataFrame(f.schema, sanitizedDF, studyVersion, studyVersionCreationDate, dictionaryVersion)
      })
  }

  private def addCQDGId(f: S3File, buildIds: String => String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val df = EtlUtils.readCsvFile(s"s3a://cqdg/${f.key}")

    val (enhancedDF: DataFrame, entityType: String) = f.schema match {
      case "familyhistory" => (df
        .withColumn("cqdg_entity", lit("family_history"))
        .withColumn(
          "cqdg_hash",
          sha1(concat_ws("_", lit("family_history"), col("study_id"), col("submitter_donor_id"), col("submitter_family_condition_id"))))
        ,"family_history")
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
      case "familyrelationship" => (df
        .withColumn("cqdg_entity", lit("family_relationship"))
        .withColumn(
          "cqdg_hash",
          sha1(concat_ws("_", lit("family_relationship"), col("study_id"), col("submitter_family_id"), col("submitter_donor_id_1"), col("submitter_donor_id_2")))),
        "family_relationship")
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
      case "dataaccess" => (df, "data_access") // ignored
      case _ => throw new RuntimeException(s"Could not find the corresponding schema to the given file ${f.filename}")
    }

    val result = if(enhancedDF.columns.contains("cqdg_hash")) {
    val idServicePayload = enhancedDF.select("cqdg_hash", "cqdg_entity").as[(String, String)].collect().toMap
    val jsonResponse = buildIds(gson.toJson(JavaConverters.mapAsJavaMap(idServicePayload)))
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

  def getOntologyDfs(files: Seq[S3File])(implicit spark: SparkSession): Map[String, DataFrame] = {
    files.flatMap(f => f.filename match {
      case "hpo_terms.json.gz" => Some("hpo" -> spark.read.json(s"s3a://cqdg/${f.key}"))
      case "mondo_terms.json.gz" => Some("mondo" -> spark.read.json(s"s3a://cqdg/${f.key}"))
      case "icd_terms.json.gz" => Some("icd" -> spark.read.json(s"s3a://cqdg/${f.key}"))
      case _ => None
    }).toMap
  }

  def filesToDf(fileList: List[S3File])(implicit sparkSession: SparkSession): List[DataFrame] = {
    fileList.map(f => EtlUtils.readCsvFile(s"s3a://cqdg/${f.key}"))
  }

  def loadSchemas(): Map[String, List[Schema]] = {
    val schemasPerVersion = new mutable.HashMap[String, List[Schema]]

    val response: Identity[Response[Either[String, String]]] = dictionaryRequest(s"dictionaries?name=$dictionaryName")

    if (StatusCode.Ok == response.code && response.body.toString.trim.nonEmpty) {
      val jsonResponse: JsonArray = JsonParser.parseString(response.body.right.get).getAsJsonArray
      jsonResponse.forEach(el => {
        val version = el.getAsJsonObject.get("version").getAsString
        schemasPerVersion.put(version, loadSchemaVersion(version))
      })
    } else {
      throw new RuntimeException(s"Failed to retrieve Lectern's versions for $dictionaryName.\n${response.body.left.get}")
    }

    schemasPerVersion.toMap
  }

  private def loadSchemaVersion(version: String): List[Schema] = {
    val response: Identity[Response[Either[String, String]]] =
      dictionaryRequest(s"dictionaries?name=$dictionaryName&version=$version")

    if (StatusCode.Ok == response.code && response.body.toString.trim.length > 0) {
      getSchemaList(response.body.right.get)
    } else {
      throw new RuntimeException(s"Failed to retrieve Lectern's schemas for version $version of $dictionaryName.\n${response.body.left.get}")
    }

  }

  def getSchemaList(responseString: String): List[Schema] = {
    val schemas: mutable.MutableList[Schema] = mutable.MutableList()

    val jsonResponse: JsonArray = JsonParser.parseString(responseString).getAsJsonArray
    jsonResponse.forEach(el => {
      val jsonSchemas = el.getAsJsonObject.get("schemas").getAsJsonArray
      jsonSchemas.forEach(x => {
        val entityType = sanitize(x.getAsJsonObject.get("name").getAsString)
        schemas += Schema(entityType, getSchemaFields(entityType, x.getAsJsonObject.get("fields").getAsJsonArray))
      })
    })

    // Add the file schema which is used for mapping the genomic files to the clinical data
    schemas += Schema("file", Seq("submitter_biospecimen_id", "submitter_donor_id", "study_id", "internal_file_id", "file_name",
      "data_category", "data_type", "is_harmonized", "experimental_strategy", "data_access", "file_format", "platform", "variant_class"))

    schemas.toList
  }

  private def getSchemaFields(entityType: String, fields: JsonArray): List[String] = {
    val fieldsList: mutable.MutableList[String] = mutable.MutableList()
    fields.forEach(field => fieldsList += field.getAsJsonObject.get("name").getAsString)
    fieldsList += s"internal_${entityType}_id"
    fieldsList.toList
  }

  private def dictionaryRequest(urlSuffix: String): Identity[Response[Either[String, String]]] = {
    val backend = HttpURLConnectionBackend()

    val url = if(dictionaryURL.endsWith("/")) s"$dictionaryURL$urlSuffix" else s"$dictionaryURL/$urlSuffix"

    // response.body : Left(errorMessage), Right(body)
    val response = basicRequest
      .auth.basic(dictionaryUsername, dictionaryPassword)
      .get(uri"${url}")
      .send(backend)

    backend.close

    response
  }
}

case class Schema(name: String, columns: Seq[String])

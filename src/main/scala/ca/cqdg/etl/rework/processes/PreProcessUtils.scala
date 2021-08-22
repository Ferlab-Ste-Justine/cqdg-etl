package ca.cqdg.etl.rework.processes

import ca.cqdg.etl.rework.EtlUtils.sanitize
import com.google.gson.Gson
import org.apache.spark.sql.functions.{col, concat_ws, lit, sha1}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters

object PreProcessUtils {

  val gson: Gson = new Gson()

  def addCQDGId(name: String, df: DataFrame, buildIds: String => String)(implicit spark: SparkSession): DataFrame = {
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

}

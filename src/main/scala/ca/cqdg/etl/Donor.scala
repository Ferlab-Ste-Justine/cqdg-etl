package ca.cqdg.etl

import ca.cqdg.etl.model.NamedDataFrame
import ca.cqdg.etl.utils.EtlUtils._
import ca.cqdg.etl.utils.EtlUtils.columns._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Donor {
  def run(broadcastStudies: Broadcast[DataFrame], dfList: List[NamedDataFrame], outputPath: String)(implicit spark: SparkSession): Unit = {
    write(build(broadcastStudies, dfList), outputPath)
  }

  def build(broadcastStudies: Broadcast[DataFrame], dfList: List[NamedDataFrame])(implicit spark: SparkSession): DataFrame = {
    val (donor, diagnosisPerDonorAndStudy, phenotypesPerDonorAndStudy, biospecimenWithSamples, file) = loadAll(dfList);

    import spark.implicits._

    val fileWithRenamedColumns = file
      .select( cols =
        $"*",
        $"file_name" as "file_name_keyword",
        $"file_name" as "file_name_ngrams",
        fileSize)
      .withColumnRenamed("variant_class", "file_variant_class")

    val fileWithBiospecimen: DataFrame = fileWithRenamedColumns
      .join(biospecimenWithSamples, $"file.submitter_biospecimen_id" === $"biospecimenWithSamples.submitter_biospecimen_id", "left")
      .drop($"biospecimenWithSamples.submitter_biospecimen_id")

    val filesPerDonorAndStudy = fileWithBiospecimen
      .groupBy($"file.submitter_donor_id", $"file.study_id")
      .agg(
        collect_list(
          struct(fileWithBiospecimen.columns.filterNot(List("study_id", "submitter_donor_id").contains(_)).map(col) : _*)
        ) as "files_per_donor_per_study"
      )
      .as("fileGroup")

    val donorStudyJoin = donor
      .join(broadcastStudies.value, $"donor.study_id" === $"study.study_id")
      .select( cols =
        $"donor.*",
        array(struct("study.*")).as("study"),
        $"familyConditions" as "familyHistory",
        $"exposures" as "exposure"
      )
      .as("donorWithStudy")

    val result = donorStudyJoin
      .join(diagnosisPerDonorAndStudy, $"donorWithStudy.study_id" === $"diagnosisGroup.study_id" && $"donorWithStudy.submitter_donor_id" === $"diagnosisGroup.submitter_donor_id", "left")
      .join(phenotypesPerDonorAndStudy, $"donorWithStudy.study_id" === $"phenotypeGroup.study_id" && $"donorWithStudy.submitter_donor_id" === $"phenotypeGroup.submitter_donor_id", "left")
      .join(filesPerDonorAndStudy, $"donorWithStudy.study_id" === $"fileGroup.study_id" && $"donorWithStudy.submitter_donor_id" === $"fileGroup.submitter_donor_id", "left")
      .select( cols =
        $"donorWithStudy.*",
        $"diagnosis_per_donor_per_study" as "diagnoses",
        $"phenotypes_per_donor_per_study" as "phenotypes",
        $"files_per_donor_per_study" as "files"
      )

    val studyNDF: NamedDataFrame = getDataframe("study", dfList)
    //result.printSchema()
    result
      .withColumn("dictionary_version", lit(studyNDF.dictionaryVersion))
      .withColumn("study_version", lit(studyNDF.studyVersion))
      .withColumn("study_version_creation_date", lit(studyNDF.studyVersionCreationDate))
  }

  def write(donors: DataFrame, outputPath: String)(implicit spark: SparkSession): Unit = {
    donors
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("study_id", "dictionary_version", "study_version", "study_version_creation_date")
      .json(outputPath)
  }
}

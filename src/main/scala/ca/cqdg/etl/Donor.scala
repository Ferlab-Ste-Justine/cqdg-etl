package ca.cqdg.etl

import ca.cqdg.etl.EtlUtils.columns._
import ca.cqdg.etl.EtlUtils.{loadBiospecimens, loadDiagnoses, loadDonors, loadPhenotypes, readCsvFile}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Donor {
  def run(broadcastStudies: Broadcast[DataFrame], inputPath: String, outputPath: String)(implicit spark: SparkSession): Unit = {
    //build(broadcastStudies, inputPath)
    write(build(broadcastStudies, inputPath), outputPath)
  }

  def build(broadcastStudies: Broadcast[DataFrame], inputPath: String)(implicit spark: SparkSession): DataFrame = {
    //TODO: Pass filename as parameters?
    val donorsInput = s"$inputPath/donor.tsv"
    val familyRelationshipInput = s"$inputPath/family-relationship.tsv"
    val familyHistoryInput = s"$inputPath/family-history.tsv"
    val exposureInput = s"$inputPath/exposure.tsv"

    val diagnosisInput = s"$inputPath/diagnosis.tsv"
    val treatmentInput = s"$inputPath/treatment.tsv"
    val followUpInput = s"$inputPath/follow-up.tsv"

    val phenotypeInput = s"$inputPath/phenotype.tsv"
    val fileInput = s"$inputPath/file.tsv"
    val biospecimenInput = s"$inputPath/biospecimen.tsv"
    val sampleInput = s"$inputPath/sample_registration.tsv"

    import spark.implicits._

    val donor: DataFrame = loadDonors(donorsInput, familyRelationshipInput, familyHistoryInput, exposureInput) as "donor"
    val biospecimenWithSamples: DataFrame = loadBiospecimens(biospecimenInput, sampleInput) as "biospecimenWithSamples"
    val diagnosisPerDonorAndStudy: DataFrame = loadDiagnoses(diagnosisInput, treatmentInput, followUpInput)
    val phenotypesPerDonorAndStudy: DataFrame = loadPhenotypes(phenotypeInput)

    val file: DataFrame = readCsvFile(fileInput) as "file"
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
        $"familyRelationships",
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

    // result.printSchema()
    result
  }

  def write(donors: DataFrame, outputPath: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    donors
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("study_id")
      .json(outputPath)
  }
}

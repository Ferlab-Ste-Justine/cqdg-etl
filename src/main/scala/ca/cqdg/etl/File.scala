package ca.cqdg.etl

import ca.cqdg.etl.EtlUtils.columns._
import ca.cqdg.etl.EtlUtils.{loadBiospecimens, loadDiagnoses, loadDonors, loadPhenotypes, readCsvFile}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object File {
  def run(broadcastStudies: Broadcast[DataFrame], inputPath: String, outputPath: String)(implicit spark: SparkSession): Unit = {
    //build(broadcastStudies, inputPath)
    write(build(broadcastStudies, inputPath), outputPath)
  }

  def build(broadcastStudies: Broadcast[DataFrame], inputPath: String)(implicit spark: SparkSession): DataFrame = {
    val fileInput = s"$inputPath/file.tsv"

    val donorsInput = s"$inputPath/donor.tsv"
    val familyRelationshipInput = s"$inputPath/family-relationship.tsv"
    val familyHistoryInput = s"$inputPath/family-history.tsv"
    val exposureInput = s"$inputPath/exposure.tsv"

    val diagnosisInput = s"$inputPath/diagnosis.tsv"
    val treatmentInput = s"$inputPath/treatment.tsv"
    val followUpInput = s"$inputPath/follow-up.tsv"

    val phenotypeInput = s"$inputPath/phenotype.tsv"
    val biospecimenInput = s"$inputPath/biospecimen.tsv"
    val sampleInput = s"$inputPath/sample_registration.tsv"

    import spark.implicits._

    val file: DataFrame = readCsvFile(fileInput) as "file"

    val donor: DataFrame = loadDonors(donorsInput, familyRelationshipInput, familyHistoryInput, exposureInput) as "donor"
    val biospecimenWithSamples: DataFrame = loadBiospecimens(biospecimenInput, sampleInput) as "biospecimenWithSamples"
    val diagnosisPerDonorAndStudy: DataFrame = loadDiagnoses(diagnosisInput, treatmentInput, followUpInput)
    val phenotypesPerDonorAndStudy: DataFrame = loadPhenotypes(phenotypeInput)

    val fileDonors = file.as("file")
      .join(donor.as("donor"), $"file.submitter_donor_id" === $"donor.submitter_donor_id")
      .groupBy("file.study_id", "file.file_name")
      .agg(
        collect_list(
          struct( cols =
            $"donor.submitter_donor_id",
            notNullCol($"ethnicity") as "ethnicity",
            $"vital_status",
            notNullCol($"gender") as "gender",
            ageAtRecruitment,
            $"familyRelationships",
            $"familyConditions" as "familyHistory",
            $"exposures" as "exposure"
          )
        ) as "cases"
      ) as "fileWithDonors"

    val fileStudyJoin = file
      .join(broadcastStudies.value, $"file.study_id" === $"study.study_id")
      .select( cols =
        array(struct("study.*")).as("study"),
        fileId,
        fileSize,
        notNullCol($"variant_class") as "file_variant_class",
        $"file.file_name" as "file_name_keyword",
        $"file.file_name" as "file_name_ngrams",
        $"file.*")
      .drop($"variant_class")
      .as("fileWithStudy")

    val result = fileStudyJoin
      .join(diagnosisPerDonorAndStudy, $"fileWithStudy.study_id" === $"diagnosisGroup.study_id" && $"fileWithStudy.submitter_donor_id" === $"diagnosisGroup.submitter_donor_id", "left")
      .join(phenotypesPerDonorAndStudy, $"fileWithStudy.study_id" === $"phenotypeGroup.study_id" && $"fileWithStudy.submitter_donor_id" === $"phenotypeGroup.submitter_donor_id", "left")
      .join(fileDonors, $"fileWithStudy.study_id" === $"fileWithDonors.study_id" && $"fileWithStudy.file_name" === $"fileWithDonors.file_name")
      .join(biospecimenWithSamples, $"fileWithStudy.submitter_biospecimen_id" === $"biospecimenWithSamples.submitter_biospecimen_id", "left")
      .select( cols =
        $"fileWithDonors.cases",
        $"biospecimenWithSamples.biospecimen" as "biospecimen",
        $"diagnosis_per_donor_per_study" as "diagnoses",
        $"phenotypes_per_donor_per_study" as "phenotypes",
        $"fileWithStudy.*"
      )
      .drop($"submitter_donor_id")
      .drop($"submitter_biospecimen_id")

    //result.printSchema()
    result
  }

  def write(files: DataFrame, outputPath: String)(implicit spark: SparkSession): Unit = {
    files
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("study_id")
      .json(outputPath)
  }
}

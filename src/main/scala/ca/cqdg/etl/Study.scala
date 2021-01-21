package ca.cqdg.etl

import ca.cqdg.etl.EtlUtils.columns._
import ca.cqdg.etl.EtlUtils.{loadBiospecimens, loadDiagnoses, loadDonors, loadPhenotypes, readCsvFile}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Study {
  def run(broadcastStudies: Broadcast[DataFrame], inputPath: String, outputPath: String)(implicit spark: SparkSession): Unit = {
    //build(broadcastStudies, inputPath)
    write(build(broadcastStudies, inputPath), outputPath)
  }

  def build(broadcastStudies: Broadcast[DataFrame], inputPath: String)(implicit spark: SparkSession): DataFrame = {
    //TODO: Pass filename as parameters?
    val donorsInput = s"$inputPath/donor.tsv"
    val diagnosisInput = s"$inputPath/diagnosis.tsv"
    val phenotypeInput = s"$inputPath/phenotype.tsv"
    val fileInput = s"$inputPath/file.tsv"
    val biospecimenInput = s"$inputPath/biospecimen.tsv"
    val sampleInput = s"$inputPath/sample_registration.tsv"

    import spark.implicits._

    val donor: DataFrame = readCsvFile(donorsInput) as "donor"
    val diagnosis: DataFrame = readCsvFile(diagnosisInput) as "diagnosis"

    val phenotypesPerDonorAndStudy: DataFrame = loadPhenotypes(phenotypeInput)
    val diagnosisPerDonorAndStudy: DataFrame = diagnosis
      .groupBy($"diagnosis.submitter_donor_id", $"diagnosis.study_id")
      .agg(
        collect_list(
          struct(cols =
            $"diagnosis.diagnosis_mondo_term" as "mondo_term",
            $"diagnosis.diagnosis_mondo_term" as "mondo_term_keyword",
            $"diagnosis.diagnosis_ICD_term" as "icd_term",
            $"diagnosis.diagnosis_ICD_term" as "icd_term_keyword",
            $"diagnosis.diagnosis_ICD_category" as "icd_category",
            $"diagnosis.diagnosis_ICD_category" as "icd_category_keyword",
            $"diagnosis.age_at_diagnosis",
          )
        ) as "diagnoses"
      ) as "diagnosisGroup"

    val donorWithPhenotypesAndDiagnosesPerStudy: DataFrame = donor
      .join(diagnosisPerDonorAndStudy, $"donor.study_id" === $"diagnosisGroup.study_id" && $"donor.submitter_donor_id" === $"diagnosisGroup.submitter_donor_id", "left")
      .join(phenotypesPerDonorAndStudy, $"donor.study_id" === $"phenotypeGroup.study_id" && $"donor.submitter_donor_id" === $"phenotypeGroup.submitter_donor_id", "left")
      .drop($"diagnosisGroup.study_id")
      .drop($"diagnosisGroup.submitter_donor_id")
      .drop($"phenotypeGroup.study_id")
      .groupBy($"study_id")
      .agg(
        collect_list(
          struct(cols =
            $"donor.submitter_donor_id",
            notNullCol($"ethnicity") as "ethnicity",
            $"vital_status",
            notNullCol($"gender") as "gender",
            ageAtRecruitment,
            $"diagnosisGroup.*",
            $"phenotypeGroup.phenotypes_per_donor_per_study" as "phenotypes"
          )
        ) as "donors"
      ) as "donorsGroup"

    val file: DataFrame = readCsvFile(fileInput) as "file"
    val biospecimenWithSamples: DataFrame = loadBiospecimens(biospecimenInput, sampleInput) as "biospecimenWithSamples"
    val fileWithBiospecimenPerStudy: DataFrame = file
      .join(biospecimenWithSamples, $"file.submitter_biospecimen_id" === $"biospecimenWithSamples.submitter_biospecimen_id", "left")
      .drop($"biospecimenWithSamples.study_id")
      .drop($"biospecimenWithSamples.submitter_biospecimen_id")
      .drop($"file.submitter_biospecimen_id")
      .groupBy($"study_id")
      .agg(
        collect_list(
          struct(cols =
            $"file.*",
            $"biospecimenWithSamples.*"
          )
        ) as "files"
      ) as "filesGroup"

    val result = broadcastStudies.value
      .join(donorWithPhenotypesAndDiagnosesPerStudy, $"study.study_id" === $"donorsGroup.study_id", "left")
      .join(fileWithBiospecimenPerStudy, $"study.study_id" === $"filesGroup.study_id", "left")
      .drop($"donorsGroup.study_id")
      .drop($"filesGroup.study_id")

    result.printSchema()
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


package bio.ferlab.cqdg.etl

import java.util

import bio.ferlab.cqdg.etl.EtlUtils.columns._
import bio.ferlab.cqdg.etl.EtlUtils.{loadDiagnoses, loadPhenotypes, readCsvFile}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object File {
  def run(broadcastStudies: Broadcast[DataFrame], inputPath: String, outputPath: String)(implicit spark: SparkSession): Unit = {
    write(build(broadcastStudies, inputPath), outputPath)
  }

  def build(broadcastStudies: Broadcast[DataFrame], inputPath: String)(implicit spark: SparkSession): DataFrame = {
    val fileInput = s"$inputPath/file.csv"
    val donorsInput = s"$inputPath/donor.csv"
    val diagnosisInput = s"$inputPath/diagnosis.csv"
    val phenotypeInput = s"$inputPath/phenotype.csv"
    val biospecimenInput = s"$inputPath/biospecimen.csv"

    import spark.implicits._

    val file: DataFrame = readCsvFile(fileInput) as "file"
    val biospecimen: DataFrame = readCsvFile(biospecimenInput) as "biospecimen"
    val donor: DataFrame = readCsvFile(donorsInput) as "donor"

    val diagnosisPerDonorAndStudy = loadDiagnoses(diagnosisInput)
    val phenotypesPerDonorAndStudy = loadPhenotypes(phenotypeInput)

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
            ageAtRecruitment)
        ) as "cases"
      ) as "fileWithDonors"

    val fileStudyJoin = file
      .join(broadcastStudies.value, $"file.study_id" === $"study.study_id")
      .select( cols =
        struct("study.*").as("study"),
        fileId,
        fileSize,
        notNullCol($"variant_class") as "file_variant_class",
        $"file.file_name" as "file_name_keyword",
        $"file.file_name" as "file_name_ngrams",
        $"file.*")
      .drop($"variant_class")
      .as("fileWithStudy")

    fileStudyJoin
      .join(diagnosisPerDonorAndStudy, $"fileWithStudy.study.study_id" === $"diagnosisGroup.study_id" && $"fileWithStudy.submitter_donor_id" === $"diagnosisGroup.submitter_donor_id", "left")
      .join(phenotypesPerDonorAndStudy, $"fileWithStudy.study.study_id" === $"phenotypeGroup.study_id" && $"fileWithStudy.submitter_donor_id" === $"phenotypeGroup.submitter_donor_id", "left")
      .join(fileDonors, $"fileWithStudy.study.study_id" === $"fileWithDonors.study_id" && $"fileWithStudy.file_name" === $"fileWithDonors.file_name")
      .join(biospecimen, $"fileWithStudy.submitter_biospecimen_id" === $"biospecimen.submitter_biospecimen_id", "left")
      .select( cols =
        $"fileWithDonors.cases",
        struct( cols =
          $"biospecimen.submitter_biospecimen_id",
          $"sample_type",
          $"tumor_normal_designation",
          $"biospecimen_tissue_source" as "tissue_source",
          $"biospecimen_type" as "type",
          isCancer,
          $"biospecimen_anatomic_location" as "anatomic_location",
          $"diagnosis_ICD_term" as "icd_term",
          $"diagnosis_ICD_term" as "icd_term_keyword"
        ).as("biospecimen"),
        $"diagnosis_per_donor_per_study" as "diagnoses",
        $"phenotypes_per_donor_per_study" as "phenotypes",
        $"fileWithStudy.*"
      )
      .drop($"submitter_donor_id")
      .drop($"submitter_biospecimen_id")
  }

  def write(files: DataFrame, outputPath: String)(implicit spark: SparkSession): Unit = {
    files
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("study_id")
      .json(outputPath)
  }
}

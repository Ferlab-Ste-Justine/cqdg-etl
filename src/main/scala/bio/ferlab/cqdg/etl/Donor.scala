
package bio.ferlab.cqdg.etl

import bio.ferlab.cqdg.etl.EtlUtils.columns._
import bio.ferlab.cqdg.etl.EtlUtils.{loadDiagnoses, loadPhenotypes, readCsvFile}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Donor {
  def run(broadcastStudies: Broadcast[DataFrame], inputPath: String, outputPath: String)(implicit spark: SparkSession): Unit = {
    write(build(broadcastStudies, inputPath), outputPath)
  }

  def build(broadcastStudies: Broadcast[DataFrame], inputPath: String)(implicit spark: SparkSession): DataFrame = {
    //TODO: Pass filename as parameters?
    val donorsInput = s"$inputPath/donor.csv"
    val diagnosisInput = s"$inputPath/diagnosis.csv"
    val phenotypeInput = s"$inputPath/phenotype.csv"
    val fileInput = s"$inputPath/file.csv"

    import spark.implicits._


    val donor: DataFrame = readCsvFile(donorsInput) as "donor"
    val file: DataFrame = readCsvFile(fileInput) as "file"

    val diagnosisPerDonorAndStudy = loadDiagnoses(diagnosisInput)
    val phenotypesPerDonorAndStudy = loadPhenotypes(phenotypeInput)

    val filesPerDonorAndStudy = file
      .groupBy("submitter_donor_id", "study_id")
      .agg(
        collect_list(
          struct( cols =
            $"file_name",
            $"file_name" as "file_name_keyword",
            $"file_name" as "file_name_ngrams",
            $"submitter_biospecimen_id",
            $"data_category",
            $"data_type",
            $"file_format",
            $"platform",
            $"variant_class",
            $"data_access",
            $"is_harmonized",
            $"experimental_strategy",
            fileId,
            fileSize
          )
        ) as "files_per_donor_per_study"
      )
      .as("fileGroup")

    val donorStudyJoin = donor
      .join(broadcastStudies.value, $"donor.study_id" === $"study.study_id")
      .select( cols =
        donorId,
        $"study.study_id" as "study_id",
        array(struct("study.*")).as("study"),
        $"donor.submitter_donor_id",
        notNullCol($"ethnicity") as "ethnicity",
        $"vital_status",
        notNullCol($"gender") as "gender",
        ageAtRecruitment)
      .as("donorWithStudy")

    donorStudyJoin
      .join(diagnosisPerDonorAndStudy, $"donorWithStudy.study_id" === $"diagnosisGroup.study_id" && $"donorWithStudy.submitter_donor_id" === $"diagnosisGroup.submitter_donor_id", "left")
      .join(phenotypesPerDonorAndStudy, $"donorWithStudy.study_id" === $"phenotypeGroup.study_id" && $"donorWithStudy.submitter_donor_id" === $"phenotypeGroup.submitter_donor_id", "left")
      .join(filesPerDonorAndStudy, $"donorWithStudy.study_id" === $"fileGroup.study_id" && $"donorWithStudy.submitter_donor_id" === $"fileGroup.submitter_donor_id", "left")
      .select($"donorWithStudy.*", $"diagnosis_per_donor_per_study" as "diagnoses", $"phenotypes_per_donor_per_study" as "phenotypes", $"files_per_donor_per_study" as "files")
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

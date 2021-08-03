package ca.cqdg.etl

import ca.cqdg.etl.model.NamedDataFrame
import ca.cqdg.etl.utils.EtlUtils.columns._
import ca.cqdg.etl.utils.{DataAccessUtils, SummaryUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Donor {
  def run(study: DataFrame,
          studyNDF: NamedDataFrame,
          inputData: Map[String, DataFrame],
          duoCodeDf: DataFrame,
          outputPath: String)(implicit spark: SparkSession): Unit = {
    write(build(study, studyNDF, inputData, duoCodeDf), outputPath)
  }

  def build(study: DataFrame,
            studyNDF: NamedDataFrame,
            data: Map[String, DataFrame],
            duoCodeDf: DataFrame
           )(implicit spark: SparkSession): DataFrame = {

    val donor = data("donor").as("donor")
    val diagnosisPerDonorAndStudy = data("diagnosisPerDonorAndStudy").as("diagnosisGroup")
    val phenotypesPerStudyIdAndDonor = data("phenotypesPerStudyIdAndDonor").as("phenotypeGroup")
    val biospecimenWithSamples = data("biospecimenWithSamples").as("biospecimenWithSamples")
    val treatmentsPerDonorAndStudy = data("treatmentsPerDonorAndStudy").as("treatmentsPerDonorAndStudy")
    val exposuresPerDonorAndStudy = data("exposuresPerDonorAndStudy").as("exposuresPerDonorAndStudy")
    val followUpsPerDonorAndStudy = data("followUpsPerDonorAndStudy").as("followUpsPerDonorAndStudy")
    val familyHistoryPerDonorAndStudy = data("familyHistoryPerDonorAndStudy").as("familyHistoryPerDonorAndStudy")
    val familyRelationshipPerDonorAndStudy = data("familyRelationshipPerDonorAndStudy").as("familyRelationshipPerDonorAndStudy")
    val file = data("file").as("file")

    import spark.implicits._

    val (donorPerFile, _, _, allStudiesAndDonorsCombinations) = SummaryUtils.prepareSummaryDataFrames(donor, file)
    val summaryByCategory = SummaryUtils.computeFilesByField(donorPerFile, allStudiesAndDonorsCombinations, "data_category").as("summaryByCategory")
    val summaryByStrategy = SummaryUtils.computeFilesByField(donorPerFile, allStudiesAndDonorsCombinations, "experimental_strategy").as("summaryByStrategy")
    val (summaryOfClinicalDataAvailable, summaryOfClinicalDataAvailableOnly) = SummaryUtils.computeAllClinicalDataAvailablePerDonor(allStudiesAndDonorsCombinations, diagnosisPerDonorAndStudy, phenotypesPerStudyIdAndDonor, treatmentsPerDonorAndStudy, exposuresPerDonorAndStudy, followUpsPerDonorAndStudy, familyHistoryPerDonorAndStudy, familyRelationshipPerDonorAndStudy)

    val summaryGroup = summaryByCategory
      .join(summaryByStrategy, Seq("study_id","submitter_donor_id"))
      .join(summaryOfClinicalDataAvailable, Seq("study_id","submitter_donor_id"))
      .join(summaryOfClinicalDataAvailableOnly, Seq("study_id","submitter_donor_id"))
      .filter(col("study_id").isNotNull)
      .filter(col("submitter_donor_id").isNotNull)
      .groupBy($"study_id", $"submitter_donor_id")
      .agg(
        first(
          struct(cols =
            $"summaryByCategory.data_category",
            $"summaryByStrategy.experimental_strategy",
            $"summaryOfClinicalDataAvailable.clinical_data_available",
            $"summaryOfClinicalDataAvailableOnly.clinical_data_available_only",
          )
        ).as("summary")
      ).as("summaryGroup")

    val fileWithRenamedColumns = file
      .select( cols =
        $"*",
        $"file_name" as "file_name_keyword",
        $"file_name" as "file_name_ngrams",
        fileSize)
      .withColumnRenamed("variant_class", "file_variant_class")

    val fileWithBiospecimen: DataFrame = fileWithRenamedColumns
      .join(biospecimenWithSamples, Seq("submitter_biospecimen_id"), "left")
      .drop($"biospecimenWithSamples.file_name")
      .drop($"biospecimenWithSamples.file_name_keyword")
      .drop($"biospecimenWithSamples.file_name_ngrams")

    val filesPerDonorAndStudy = fileWithBiospecimen
      .groupBy($"file.submitter_donor_id", $"file.study_id")
      .agg(
        collect_list(
          struct(fileWithBiospecimen.columns.filterNot(List("study_id", "submitter_donor_id", "file_name", "file_name_keyword", "file_name_ngrams").contains(_)).map(col) : _*)
        ) as "files_per_donor_per_study"
      )
      .as("fileGroup")

    val donorStudyJoin = donor
      .join(study, $"donor.study_id" === $"study.study_id")
      .select( cols =
        $"donor.*",
        array(struct("study.*")).as("study"),
        $"familyConditions" as "familyHistory",
      )
      .as("donorWithStudy")

    val result = donorStudyJoin
      .join(diagnosisPerDonorAndStudy, Seq("study_id", "submitter_donor_id"), "left")
      .join(phenotypesPerStudyIdAndDonor, Seq("study_id", "submitter_donor_id"), "left")
      .join(filesPerDonorAndStudy, Seq("study_id", "submitter_donor_id"), "left")
      .join(summaryGroup, Seq("study_id", "submitter_donor_id"), "left")
      .select( cols =
        $"donorWithStudy.*",
        $"diagnoses",
        $"mondo",
        $"icd",
        $"observed_phenotype_tagged",
        $"not_observed_phenotype_tagged",
        $"observed_phenotypes",
        $"non_observed_phenotypes",
        $"files_per_donor_per_study" as "files",
        $"summaryGroup.summary",
      )

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

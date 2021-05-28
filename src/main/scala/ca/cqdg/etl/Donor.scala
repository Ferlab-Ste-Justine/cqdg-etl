package ca.cqdg.etl

import ca.cqdg.etl.model.NamedDataFrame
import ca.cqdg.etl.utils.EtlUtils._
import ca.cqdg.etl.utils.EtlUtils.columns._
import ca.cqdg.etl.utils.{DataAccessUtils, SummaryUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Donor {
  def run(broadcastStudies: Broadcast[DataFrame],
          dfList: List[NamedDataFrame],
          ontologyDf: Map[String, DataFrame],
          outputPath: String)(implicit spark: SparkSession): Unit = {
    write(build(broadcastStudies, dfList, ontologyDf), outputPath)
  }

  def build(broadcastStudies: Broadcast[DataFrame],
            dfList: List[NamedDataFrame],
            ontologyDf: Map[String, DataFrame]
           )(implicit spark: SparkSession): DataFrame = {
    val (dataAccess, donor, diagnosisPerDonorAndStudy, phenotypesPerDonorAndStudy, biospecimenWithSamples, file, treatmentsPerDonorAndStudy, exposuresPerDonorAndStudy, followUpsPerDonorAndStudy, familyHistoryPerDonorAndStudy, familyRelationshipPerDonorAndStudy) = loadAll(dfList)(ontologyDf)

    import spark.implicits._

    val dataAccessGroup = DataAccessUtils.computeDataAccessByEntityType(dataAccess, "donor", "submitter_donor_id")

    val (donorPerFile, _, _, allStudiesAndDonorsCombinations) = SummaryUtils.prepareSummaryDataFrames(donor, file)
    val summaryByCategory = SummaryUtils.computeFilesByField(donorPerFile, allStudiesAndDonorsCombinations, "data_category").as("summaryByCategory")
    val summaryByStrategy = SummaryUtils.computeFilesByField(donorPerFile, allStudiesAndDonorsCombinations, "experimental_strategy").as("summaryByStrategy")
    val summaryOfClinicalDataAvailable = SummaryUtils.computeAllClinicalDataAvailablePerDonor(allStudiesAndDonorsCombinations, diagnosisPerDonorAndStudy, phenotypesPerDonorAndStudy, treatmentsPerDonorAndStudy, exposuresPerDonorAndStudy, followUpsPerDonorAndStudy, familyHistoryPerDonorAndStudy, familyRelationshipPerDonorAndStudy)
      .as("summaryOfClinicalDataAvailable")

    val summaryGroup = summaryByCategory
      .join(summaryByStrategy, Seq("study_id","submitter_donor_id"))
      .join(summaryOfClinicalDataAvailable, Seq("study_id","submitter_donor_id"))
      .filter(col("study_id").isNotNull)
      .filter(col("submitter_donor_id").isNotNull)
      .groupBy($"study_id", $"submitter_donor_id")
      .agg(
        collect_list(
          struct(cols =
            $"summaryByCategory.data_category",
            $"summaryByStrategy.experimental_strategy",
            $"summaryOfClinicalDataAvailable.clinical_data_available",
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
      .join(diagnosisPerDonorAndStudy, Seq("study_id", "submitter_donor_id"), "left")
      .join(phenotypesPerDonorAndStudy, Seq("study_id", "submitter_donor_id"), "left")
      .join(filesPerDonorAndStudy, Seq("study_id", "submitter_donor_id"), "left")
      .join(summaryGroup, Seq("study_id", "submitter_donor_id"), "left")
      .join(dataAccessGroup, Seq("submitter_donor_id"), "left")
      .select( cols =
        $"donorWithStudy.*",
        $"diagnoses",
        $"phenotypes",
        $"files_per_donor_per_study" as "files",
        $"summaryGroup.summary",
        $"dataAccessGroup.data_access"
      )

    val studyNDF: NamedDataFrame = getDataframe("study", dfList)
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

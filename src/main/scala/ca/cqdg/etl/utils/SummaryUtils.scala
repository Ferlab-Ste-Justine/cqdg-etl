package ca.cqdg.etl.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, countDistinct, lit, struct}

object SummaryUtils {

  val CROSS_JOIN_PARTITION_SIZE = 10

  def prepareSummaryDataFrames(donor: DataFrame, file: DataFrame): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    val donorPerFile = donor
      .join(file, Seq("study_id", "submitter_donor_id"))

    val allDistinctStudies = donorPerFile
      .select("study_id")
      .distinct()

    val allDistinctDonors = donor
      .select("submitter_donor_id")
      .distinct()

    // without .repartition(..) => it's infinite loop when write/show this DF
    val allStudiesAndDonorsCombinations = allDistinctStudies.repartition(CROSS_JOIN_PARTITION_SIZE)
      .crossJoin(allDistinctDonors.repartition(CROSS_JOIN_PARTITION_SIZE))

    (donorPerFile, allDistinctStudies, allDistinctDonors, allStudiesAndDonorsCombinations)
  }

  def computeDonorsAndFilesByField(donorPerFile: DataFrame, allDistinctStudies: DataFrame, fieldName: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val allDistinctFieldToCompute = donorPerFile
      .select(fieldName)
      .distinct()

    // without .repartition(..) => it's infinite loop when write/show this DF
    val allCombinations = allDistinctStudies.repartition(CROSS_JOIN_PARTITION_SIZE)
      .crossJoin(allDistinctFieldToCompute.repartition(CROSS_JOIN_PARTITION_SIZE))

    donorPerFile
      .join(allCombinations, Seq("study_id", fieldName), "full") // line can be removed if we don't want to count allCombinations
      .filter(col(fieldName).isNotNull)
      .groupBy($"study_id", col(fieldName))
      .agg(
        countDistinct($"submitter_donor_id").as("donors"),
        countDistinct($"file_name").as("files")
      )
      .groupBy($"study_id")  // mandatory we need one entry per study_id in the end result
      .agg(
        collect_list(
          struct(cols =
            col(fieldName).as("key"),
            $"donors",
            $"files",
          )
        ).as(fieldName)
      )
  }

  def computeFilesByField(donorPerFile: DataFrame, allStudiesAndDonorsCombinations: DataFrame, fieldName: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val allDistinctFieldToCompute = donorPerFile
      .select(fieldName)
      .distinct()

    // without .repartition(..) => it's infinite loop when write/show this DF
    val allCombinations = allStudiesAndDonorsCombinations
      .crossJoin(allDistinctFieldToCompute.repartition(CROSS_JOIN_PARTITION_SIZE))

    donorPerFile
      .join(allCombinations, Seq("study_id", "submitter_donor_id", fieldName), "full") // line can be removed if we don't want to count allCombinations
      .filter(col(fieldName).isNotNull)
      .groupBy($"study_id", $"submitter_donor_id", col(fieldName))
      .agg(
        countDistinct($"file_name").as("files")
      )
      .groupBy($"study_id", $"submitter_donor_id")  // mandatory we need one entry per study_id in the end result
      .agg(
        collect_list(
          struct(cols =
            col(fieldName).as("key"),
            $"files",
          )
        ).as(fieldName)
      )
  }

  private def computeClinicalDataAvailableForDataFrame(dataFrame: DataFrame, keyName: String, submitterDonorIdColName: String = "submitter_donor_id")(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    dataFrame
      .withColumnRenamed(submitterDonorIdColName, "submitter_donor_id")
      .groupBy($"study_id")
      .agg(
        lit(keyName).as("key"),
        countDistinct("submitter_donor_id").as("donors")
      )
  }

  def computeAllClinicalDataAvailable(diagnosisPerDonorAndStudy: DataFrame, phenotypesPerDonorAndStudy: DataFrame, treatmentsPerDonorAndStudy: DataFrame, exposuresPerDonorAndStudy: DataFrame, followUpsPerDonorAndStudy: DataFrame, familyHistoryPerDonorAndStudy: DataFrame, familyRelationshipPerDonorAndStudy: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val summaryDiagnosis = computeClinicalDataAvailableForDataFrame(diagnosisPerDonorAndStudy, "diagnosis")
    val summaryPhenotype = computeClinicalDataAvailableForDataFrame(phenotypesPerDonorAndStudy, "phenotype")
    val summaryTreatment = computeClinicalDataAvailableForDataFrame(treatmentsPerDonorAndStudy, "treatment")
    val summaryExposure = computeClinicalDataAvailableForDataFrame(exposuresPerDonorAndStudy, "exposure")
    val summaryFollowUp = computeClinicalDataAvailableForDataFrame(followUpsPerDonorAndStudy, "follow_up")
    val summaryFamilyHistory = computeClinicalDataAvailableForDataFrame(familyHistoryPerDonorAndStudy, "family_history")
    val summaryFamilyRelationship = computeClinicalDataAvailableForDataFrame(familyRelationshipPerDonorAndStudy, "family_relationship", "submitter_donor_id_1")

    val columnsToFullJoin = Seq("study_id", "key", "donors");

    summaryDiagnosis
      .join(summaryPhenotype, columnsToFullJoin, "full")
      .join(summaryTreatment, columnsToFullJoin,"full")
      .join(summaryExposure,columnsToFullJoin,"full")
      .join(summaryFollowUp, columnsToFullJoin,"full")
      .join(summaryFamilyHistory, columnsToFullJoin,"full")
      .join(summaryFamilyRelationship, columnsToFullJoin,"full")
      .groupBy($"study_id")
      .agg(
        collect_list(
          struct(cols =
            $"key",
            $"donors",
          )
        ).as("clinical_data_available")
      )
  }


  private def computeClinicalDataAvailableForDataFramePerDonor(allStudiesAndDonorsCombinations: DataFrame, dataFrame: DataFrame, keyName: String, submitterDonorIdColName: String = "submitter_donor_id")(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val columnsToFullJoin = Seq("study_id", "submitter_donor_id")

    dataFrame
      .withColumnRenamed(submitterDonorIdColName, "submitter_donor_id")
      .join(allStudiesAndDonorsCombinations, columnsToFullJoin, "full")
      .groupBy(columnsToFullJoin.map(col):_*)
      .agg(
        lit(keyName).as("key"),
        EtlUtils.columns.toBoolean(countDistinct("*")).as("available")
      )
  }

  def computeAllClinicalDataAvailablePerDonor(allStudiesAndDonorsCombinations: DataFrame, diagnosisPerDonorAndStudy: DataFrame, phenotypesPerDonorAndStudy: DataFrame, treatmentsPerDonorAndStudy: DataFrame, exposuresPerDonorAndStudy: DataFrame, followUpsPerDonorAndStudy: DataFrame, familyHistoryPerDonorAndStudy: DataFrame, familyRelationshipPerDonorAndStudy: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val summaryDiagnosis = computeClinicalDataAvailableForDataFramePerDonor(allStudiesAndDonorsCombinations, diagnosisPerDonorAndStudy, "diagnosis")
    val summaryPhenotype = computeClinicalDataAvailableForDataFramePerDonor(allStudiesAndDonorsCombinations, phenotypesPerDonorAndStudy, "phenotype")
    val summaryTreatment = computeClinicalDataAvailableForDataFramePerDonor(allStudiesAndDonorsCombinations, treatmentsPerDonorAndStudy, "treatment")
    val summaryExposure = computeClinicalDataAvailableForDataFramePerDonor(allStudiesAndDonorsCombinations, exposuresPerDonorAndStudy, "exposure")
    val summaryFollowUp = computeClinicalDataAvailableForDataFramePerDonor(allStudiesAndDonorsCombinations, followUpsPerDonorAndStudy, "follow_up")
    val summaryFamilyHistory = computeClinicalDataAvailableForDataFramePerDonor(allStudiesAndDonorsCombinations, familyHistoryPerDonorAndStudy, "family_history")
    val summaryFamilyRelationship = computeClinicalDataAvailableForDataFramePerDonor(allStudiesAndDonorsCombinations, familyRelationshipPerDonorAndStudy, "family_relationship", "submitter_donor_id_1")

    val columnsToFullJoin = Seq("study_id", "submitter_donor_id", "key", "available");

    summaryDiagnosis
      .join(summaryPhenotype, columnsToFullJoin, "full")
      .join(summaryTreatment, columnsToFullJoin,"full")
      .join(summaryExposure,columnsToFullJoin,"full")
      .join(summaryFollowUp, columnsToFullJoin,"full")
      .join(summaryFamilyHistory, columnsToFullJoin,"full")
      .join(summaryFamilyRelationship, columnsToFullJoin,"full")
      .groupBy($"study_id", $"submitter_donor_id")
      .agg(
        collect_list(
          struct(cols =
            $"key",
            $"available",
          )
        ).as("clinical_data_available")
      )
  }

}

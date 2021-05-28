package ca.cqdg.etl

import ca.cqdg.etl.model.NamedDataFrame
import ca.cqdg.etl.utils.EtlUtils.{getDataframe, loadAll}
import ca.cqdg.etl.utils.{DataAccessUtils, EtlUtils, SummaryUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

object Study {
  def run(
           broadcastStudies: Broadcast[DataFrame],
           dfList: List[NamedDataFrame],
           ontologyDf: Map[String, DataFrame],
           outputPath: String
         )(implicit spark: SparkSession): Unit = {
    write(build(broadcastStudies, dfList,ontologyDf), outputPath)
  }

  def build(
             broadcastStudies: Broadcast[DataFrame],
             dfList: List[NamedDataFrame],
             ontologyDf: Map[String, DataFrame]
           )(implicit spark: SparkSession): DataFrame = {
    val (dataAccess, donor, diagnosisPerDonorAndStudy, phenotypesPerDonorAndStudy, biospecimenWithSamples, file, treatmentsPerDonorAndStudy, exposuresPerDonorAndStudy, followUpsPerDonorAndStudy, familyHistoryPerDonorAndStudy, familyRelationshipPerDonorAndStudy) = loadAll(dfList)(ontologyDf)

    import spark.implicits._

    val dataAccessGroup = DataAccessUtils.computeDataAccessByEntityType(dataAccess, "study", "study_id")

    val (donorPerFile, allDistinctStudies, _, _) = SummaryUtils.prepareSummaryDataFrames(donor, file)
    val summaryByCategory = SummaryUtils.computeDonorsAndFilesByField(donorPerFile, allDistinctStudies, "data_category").as("summaryByCategory")
    val summaryByStrategy = SummaryUtils.computeDonorsAndFilesByField(donorPerFile, allDistinctStudies, "experimental_strategy").as("summaryByStrategy")
    val summaryOfClinicalDataAvailable = SummaryUtils.computeAllClinicalDataAvailable(diagnosisPerDonorAndStudy, phenotypesPerDonorAndStudy, treatmentsPerDonorAndStudy, exposuresPerDonorAndStudy, followUpsPerDonorAndStudy, familyHistoryPerDonorAndStudy, familyRelationshipPerDonorAndStudy)
      .as("summaryOfClinicalDataAvailable")

    val summaryGroup = summaryByCategory
      .join(summaryByStrategy, "study_id")
      .join(summaryOfClinicalDataAvailable, "study_id")
      .filter(col("study_id").isNotNull)
      .groupBy($"study_id")
      .agg(
        collect_list(
          struct(cols =
            $"summaryByCategory.data_category",
            $"summaryByStrategy.experimental_strategy",
            $"summaryOfClinicalDataAvailable.clinical_data_available",
          )
        ).as("summary")
      ).as("summaryGroup")

    val donorWithPhenotypesAndDiagnosesPerStudy: DataFrame = donor
      .join(diagnosisPerDonorAndStudy, $"donor.study_id" === $"diagnosisGroup.study_id" && $"donor.submitter_donor_id" === $"diagnosisGroup.submitter_donor_id", "left")
      .join(phenotypesPerDonorAndStudy, $"donor.study_id" === $"phenotypeGroup.study_id" && $"donor.submitter_donor_id" === $"phenotypeGroup.submitter_donor_id", "left")
      .drop($"diagnosisGroup.study_id")
      .drop($"diagnosisGroup.submitter_donor_id")
      .drop($"phenotypeGroup.study_id")
      .drop($"phenotypeGroup.submitter_donor_id")
      .groupBy($"study_id")
      .agg(
        collect_list(
          struct(cols =
            (donor.columns.filterNot(List("study_id", "submitter_family_id").contains(_)).map(col) ++
              List($"diagnosisGroup.*", $"phenotypeGroup.phenotypes" as "phenotypes")) : _*
          )
        ) as "donors"
      ) as "donorsGroup"

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
      .join(donorWithPhenotypesAndDiagnosesPerStudy, Seq("study_id"), "left")
      .join(fileWithBiospecimenPerStudy, Seq("study_id"), "left")
      .join(summaryGroup, Seq("study_id"), "left")
      .join(dataAccessGroup, Seq("study_id"), "left")

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

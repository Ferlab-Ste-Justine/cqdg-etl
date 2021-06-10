package ca.cqdg.etl

import ca.cqdg.etl.model.NamedDataFrame
import ca.cqdg.etl.utils.EtlUtils.columns.fileSize
import ca.cqdg.etl.utils.{DataAccessUtils, SummaryUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Study {
  def run(study: DataFrame,
          studyNDF: NamedDataFrame,
          inputData: Map[String, DataFrame],
          ontologyDf: Map[String, DataFrame],
          outputPath: String
         )(implicit spark: SparkSession): Unit = {
    write(build(study, studyNDF, inputData, ontologyDf), outputPath)
  }

  def build(
             study: DataFrame,
             studyNDF: NamedDataFrame,
             data: Map[String, DataFrame],
             ontologyDf: Map[String, DataFrame]
           )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val donor = data("donor").as("donor")
    val diagnosisPerDonorAndStudy = data("diagnosisPerDonorAndStudy").as("diagnosisGroup")
    val phenotypesPerStudyIdAndDonor = data("phenotypesPerStudyIdAndDonor").as("phenotypeGroup")
    val biospecimenWithSamples = data("biospecimenWithSamples").as("biospecimenWithSamples")
    val dataAccess = data("dataAccess").as("dataAccess")
    val treatmentsPerDonorAndStudy = data("treatmentsPerDonorAndStudy").as("treatmentsPerDonorAndStudy")
    val exposuresPerDonorAndStudy = data("exposuresPerDonorAndStudy").as("exposuresPerDonorAndStudy")
    val followUpsPerDonorAndStudy = data("followUpsPerDonorAndStudy").as("followUpsPerDonorAndStudy")
    val familyHistoryPerDonorAndStudy = data("familyHistoryPerDonorAndStudy").as("familyHistoryPerDonorAndStudy")
    val familyRelationshipPerDonorAndStudy = data("familyRelationshipPerDonorAndStudy").as("familyRelationshipPerDonorAndStudy")
    val file = data("file").as("file")
    
    val dataAccessGroup = DataAccessUtils.computeDataAccessByEntityType(dataAccess, "study", "study_id")

    val (donorPerFile, allDistinctStudies, _, _) = SummaryUtils.prepareSummaryDataFrames(donor, file)
    val summaryByCategory = SummaryUtils.computeDonorsAndFilesByField(donorPerFile, allDistinctStudies, "data_category").as("summaryByCategory")
    val summaryByStrategy = SummaryUtils.computeDonorsAndFilesByField(donorPerFile, allDistinctStudies, "experimental_strategy").as("summaryByStrategy")
    val summaryOfClinicalDataAvailable = SummaryUtils.computeAllClinicalDataAvailable(diagnosisPerDonorAndStudy, phenotypesPerStudyIdAndDonor, treatmentsPerDonorAndStudy, exposuresPerDonorAndStudy, followUpsPerDonorAndStudy, familyHistoryPerDonorAndStudy, familyRelationshipPerDonorAndStudy)
      .as("summaryOfClinicalDataAvailable")

    val summaryGroup = summaryByCategory
      .join(summaryByStrategy, "study_id")
      .join(summaryOfClinicalDataAvailable, "study_id")
      .filter(col("study_id").isNotNull)
      .groupBy($"study_id")
      .agg(
        first(  // create an object, no need of an array
          struct(cols =
            $"summaryByCategory.data_category",
            $"summaryByStrategy.experimental_strategy",
            $"summaryOfClinicalDataAvailable.clinical_data_available",
          )
        ).as("summary")
      ).as("summaryGroup")

    val donorWithPhenotypesAndDiagnosesPerStudy: DataFrame = donor
      .join(diagnosisPerDonorAndStudy, Seq("study_id", "submitter_donor_id"), "left")
      .join(phenotypesPerStudyIdAndDonor, Seq("study_id", "submitter_donor_id"), "left")
      .groupBy($"study_id")
      .agg(
        collect_list(
          struct(cols =
            (donor.columns.filterNot(List("study_id", "submitter_family_id").contains(_)).map(col) ++
              List($"diagnosisGroup.*", $"observed_phenotype_tagged", $"not_observed_phenotype_tagged")) : _*
          )
        ) as "donors"
      ) as "donorsGroup"

    val fileWithSize = file
      .select( cols =
        $"*",
        fileSize)

    val fileWithBiospecimenPerStudy: DataFrame = fileWithSize
      .join(biospecimenWithSamples, $"file.submitter_biospecimen_id" === $"biospecimenWithSamples.submitter_biospecimen_id", "left")
      .drop($"biospecimenWithSamples.study_id")
      .drop($"biospecimenWithSamples.submitter_biospecimen_id")
      .drop($"file.submitter_biospecimen_id")
      .drop($"file.file_name")
      .drop($"file.file_name_keyword")
      .drop($"file.file_name_ngrams")
      .groupBy($"study_id")
      .agg(
        collect_list(
          struct(cols =
            $"file.*",
            $"file_size",
            $"biospecimenWithSamples.*"
          )
        ) as "files"
      ) as "filesGroup"

    val result = study
      .join(donorWithPhenotypesAndDiagnosesPerStudy, Seq("study_id"), "left")
      .join(fileWithBiospecimenPerStudy, Seq("study_id"), "left")
      .join(summaryGroup, Seq("study_id"), "left")
      .join(dataAccessGroup, Seq("study_id"), "left")

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

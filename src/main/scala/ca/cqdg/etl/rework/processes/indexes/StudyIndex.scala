package ca.cqdg.etl.rework.processes.indexes

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import ca.cqdg.etl.rework.models.Metadata
import ca.cqdg.etl.rework.processes.ProcessETLUtils.columns.fileSize
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class StudyIndex (study: DataFrame,
            metadata: Metadata,
            inputData: Map[String, DataFrame]
            )(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("studies")

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    inputData
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

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

    val (donorPerFile, allDistinctStudies, _, _) = Summary.prepareSummaryDataFrames(donor, file)
    val summaryByCategory = Summary.computeDonorsAndFilesByField(donorPerFile, allDistinctStudies, "data_category").as("summaryByCategory")
    val summaryByStrategy = Summary.computeDonorsAndFilesByField(donorPerFile, allDistinctStudies, "experimental_strategy").as("summaryByStrategy")
    val summaryOfClinicalDataAvailable = Summary.computeAllClinicalDataAvailable(diagnosisPerDonorAndStudy, phenotypesPerStudyIdAndDonor, treatmentsPerDonorAndStudy, exposuresPerDonorAndStudy, followUpsPerDonorAndStudy, familyHistoryPerDonorAndStudy, familyRelationshipPerDonorAndStudy)
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

    result
      .withColumn("dictionary_version", lit(metadata.dictionaryVersion))
      .withColumn("study_version", lit(metadata.studyVersion))
      .withColumn("study_version_creation_date", lit(metadata.studyVersionCreationDate))
  }
}

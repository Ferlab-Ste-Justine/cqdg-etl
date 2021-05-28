package ca.cqdg.etl

import ca.cqdg.etl.model.NamedDataFrame
import ca.cqdg.etl.utils.EtlUtils.{getDataframe, loadAll}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Study {
  def run(
           broadcastStudies: Broadcast[DataFrame],
           dfList: List[NamedDataFrame],
           ontologyDf: Map[String, DataFrame],
           outputPath: String
         )(implicit spark: SparkSession): Unit = {
    write(build(broadcastStudies, dfList,ontologyDf), outputPath)
  }

  private def prepareDonorPerFile(donor: DataFrame, file: DataFrame): (DataFrame, DataFrame) = {
    val donorPerFile = donor
      .join(file, Seq("study_id", "submitter_donor_id"))

    val allDistinctStudies = donorPerFile
      .select("study_id")
      .distinct()

    (donorPerFile, allDistinctStudies)
  }

  private def computeDonorsAndFilesByField(donorPerFile: DataFrame, allDistinctStudies: DataFrame, fieldName: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val allDistinctFieldToCompute = donorPerFile
      .select(fieldName)
      .distinct()

    val allCombinations = allDistinctStudies.repartition(allDistinctStudies.count().toInt)
      .crossJoin(allDistinctFieldToCompute.repartition(allDistinctFieldToCompute.count().toInt))
      // without .repartition(..) => it's infinite loop when write/show this DF

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

  private def computeClinicalDataAvailableForDataFrame(dataFrame: DataFrame, keyName: String, submitterDonorIdColName: String = "submitter_donor_id")(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    dataFrame
      .groupBy($"study_id")
      .agg(
        lit(keyName).as("key"),
        countDistinct(submitterDonorIdColName).as("donors")
      )
  }

  private def computeAllClinicalDataAvailable(diagnosisPerDonorAndStudy: DataFrame, phenotypesPerDonorAndStudy: DataFrame, treatmentsPerDonorAndStudy: DataFrame, exposuresPerDonorAndStudy: DataFrame, followUpsPerDonorAndStudy: DataFrame, familyHistoryPerDonorAndStudy: DataFrame, familyRelationshipPerDonorAndStudy: DataFrame)(implicit spark: SparkSession): DataFrame = {
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

  def build(
             broadcastStudies: Broadcast[DataFrame],
             dfList: List[NamedDataFrame],
             ontologyDf: Map[String, DataFrame]
           )(implicit spark: SparkSession): DataFrame = {
    val (donor, diagnosisPerDonorAndStudy, phenotypesPerDonorAndStudy, biospecimenWithSamples, file, treatmentsPerDonorAndStudy, exposuresPerDonorAndStudy, followUpsPerDonorAndStudy, familyHistoryPerDonorAndStudy, familyRelationshipPerDonorAndStudy) = loadAll(dfList)(ontologyDf)

    import spark.implicits._

    val (donorPerFile, allDistinctStudies) = prepareDonorPerFile(donor, file)
    val summaryByCategory = computeDonorsAndFilesByField(donorPerFile, allDistinctStudies, "data_category").as("summaryByCategory")
    val summaryByStrategy = computeDonorsAndFilesByField(donorPerFile, allDistinctStudies, "experimental_strategy").as("summaryByStrategy")
    val summaryOfClinicalDataAvailable = computeAllClinicalDataAvailable(diagnosisPerDonorAndStudy, phenotypesPerDonorAndStudy, treatmentsPerDonorAndStudy, exposuresPerDonorAndStudy, followUpsPerDonorAndStudy, familyHistoryPerDonorAndStudy, familyRelationshipPerDonorAndStudy)
      .as("summaryOfClinicalDataAvailable")

    val summaryGroup = summaryByCategory
      .join(summaryByStrategy, "study_id")
      .join(summaryOfClinicalDataAvailable, "study_id")
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
      .join(donorWithPhenotypesAndDiagnosesPerStudy, $"study.study_id" === $"donorsGroup.study_id", "left")
      .join(fileWithBiospecimenPerStudy, $"study.study_id" === $"filesGroup.study_id", "left")
      .join(summaryGroup, $"study.study_id" === $"summaryGroup.study_id", "left")
      .drop($"donorsGroup.study_id")
      .drop($"filesGroup.study_id")
      .drop($"summaryGroup.study_id")

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

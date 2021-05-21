package ca.cqdg.etl

import ca.cqdg.etl.model.NamedDataFrame
import ca.cqdg.etl.utils.EtlUtils.{getDataframe, loadAll}
import org.apache.spark.broadcast.Broadcast
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

  private def computeDonorsAndFilesByField(donor: DataFrame, file: DataFrame, fieldName: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    donor.as("donor")
      .join(file.as("file"), Seq("submitter_donor_id", "study_id"))
      .filter(col(fieldName).isNotNull)
      .groupBy($"study_id", col(fieldName))
      .agg(
        countDistinct($"submitter_donor_id").as("donors"),
        countDistinct($"file_name").as("files")
      ).groupBy($"study_id")  // mandatory we need one entry per study_id in the end result
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

  private def computeDonorsByField(dataFrame: DataFrame, fieldName: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    dataFrame
      .groupBy($"study_id")
      .agg(
        countDistinct($"submitter_donor_id").as("donors")
      )
      .groupBy($"study_id")
      .agg(
        collect_list(
          struct(cols =
            $"donors"
          )
        ).as(fieldName)
      )
  }

  def build(
             broadcastStudies: Broadcast[DataFrame],
             dfList: List[NamedDataFrame],
             ontologyDf: Map[String, DataFrame]
           )(implicit spark: SparkSession): DataFrame = {
    val (donor, diagnosisPerDonorAndStudy, phenotypesPerDonorAndStudy, biospecimenWithSamples, file, treatmentsPerDonorAndStudy) = loadAll(dfList)(ontologyDf)

    import spark.implicits._

    val summaryByCategory = computeDonorsAndFilesByField(donor, file, "data_category").as("summaryByCategory")
    val summaryByStrategy= computeDonorsAndFilesByField(donor, file, "experimental_strategy").as("summaryByStrategy")
    val summaryDiagnosis = computeDonorsByField(diagnosisPerDonorAndStudy, "diagnosis").as("summaryDiagnosis")
    val summaryPhenotype = computeDonorsByField(phenotypesPerDonorAndStudy, "phenotype").as("summaryPhenotype")
    val summaryTreatment = computeDonorsByField(treatmentsPerDonorAndStudy, "treatment").as("summaryTreatment")

    val summaryGroup = summaryByCategory
      .join(summaryByStrategy, "study_id")
      .join(summaryDiagnosis, "study_id")
      .join(summaryPhenotype, "study_id")
      .join(summaryTreatment, "study_id")
      .groupBy($"study_id")
      .agg(
        collect_list(
          struct(cols =
            $"summaryByCategory.data_category",
            $"summaryByStrategy.experimental_strategy",
            $"summaryDiagnosis.diagnosis",
            $"summaryPhenotype.phenotype",
            $"summaryTreatment.treatment",
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

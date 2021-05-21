package ca.cqdg.etl

import ca.cqdg.etl.model.NamedDataFrame
import ca.cqdg.etl.utils.EtlUtils.columns._
import ca.cqdg.etl.utils.EtlUtils.{getDataframe, loadAll}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object File {
  def run(
           broadcastStudies: Broadcast[DataFrame],
           dfList: List[NamedDataFrame],
           ontologyDf: Map[String, DataFrame],
           outputPath: String)(implicit spark: SparkSession): Unit = {
    write(build(broadcastStudies, dfList, ontologyDf), outputPath)
  }

  def build(
             broadcastStudies: Broadcast[DataFrame],
             dfList: List[NamedDataFrame],
             ontologyDf: Map[String, DataFrame]
           )(implicit spark: SparkSession): DataFrame = {
    val (donor, diagnosisPerDonorAndStudy, phenotypesPerDonorAndStudy, biospecimenWithSamples, file, treatmentsPerDonorAndStudy) = loadAll(dfList)(ontologyDf);

    import spark.implicits._

    val fileDonors = file.as("file")
      .join(donor.as("donor"), $"file.submitter_donor_id" === $"donor.submitter_donor_id")
      .groupBy($"file.study_id", $"file.file_name")
      .agg(
        collect_list(
          struct( cols =
            $"donor.*"
          )
        ) as "donors"
      ) as "fileWithDonors"

    val fileStudyJoin = file
      .join(broadcastStudies.value, $"file.study_id" === $"study.study_id")
      .select( cols =
        $"file.file_name" as "file_name_keyword",
        $"file.file_name" as "file_name_ngrams",
        $"file.*",
        fileSize,
        array(struct("study.*")) as "study",
        notNullCol($"variant_class") as "file_variant_class"
      )
      .drop($"variant_class")
      .as("fileWithStudy")

    val result = fileStudyJoin
      .join(diagnosisPerDonorAndStudy, $"fileWithStudy.study_id" === $"diagnosisGroup.study_id" && $"fileWithStudy.submitter_donor_id" === $"diagnosisGroup.submitter_donor_id", "left")
      .join(phenotypesPerDonorAndStudy, $"fileWithStudy.study_id" === $"phenotypeGroup.study_id" && $"fileWithStudy.submitter_donor_id" === $"phenotypeGroup.submitter_donor_id", "left")
      .join(fileDonors, $"fileWithStudy.study_id" === $"fileWithDonors.study_id" && $"fileWithStudy.file_name" === $"fileWithDonors.file_name")
      .join(biospecimenWithSamples, $"fileWithStudy.submitter_biospecimen_id" === $"biospecimenWithSamples.submitter_biospecimen_id", "left")
      .select( cols =
        $"fileWithStudy.*",
        $"fileWithDonors.donors",
        $"biospecimenWithSamples.biospecimen" as "biospecimen",
        $"diagnosis_per_donor_per_study" as "diagnoses",
        $"phenotypes"
      )
      .drop($"submitter_donor_id")
      .drop($"submitter_biospecimen_id")

    val studyNDF: NamedDataFrame = getDataframe("study", dfList)
    //result.printSchema()
    result
      .withColumn("dictionary_version", lit(studyNDF.dictionaryVersion))
      .withColumn("study_version", lit(studyNDF.studyVersion))
      .withColumn("study_version_creation_date", lit(studyNDF.studyVersionCreationDate))
  }

  def write(files: DataFrame, outputPath: String)(implicit spark: SparkSession): Unit = {
    files
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("study_id", "dictionary_version", "study_version", "study_version_creation_date")
      .json(outputPath)
  }
}

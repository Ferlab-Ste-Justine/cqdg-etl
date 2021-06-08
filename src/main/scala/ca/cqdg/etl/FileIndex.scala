package ca.cqdg.etl

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import ca.cqdg.etl.model.NamedDataFrame
import ca.cqdg.etl.utils.EtlUtils.columns._
import ca.cqdg.etl.utils.EtlUtils.getDataframe
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class FileIndex(studyDf: DataFrame,
                studyNDF: NamedDataFrame,
                inputData: Map[String, DataFrame])(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("files")

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    inputData
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {

    val donor = data("donor").as("donor")
    val diagnosisPerDonorAndStudy = data("diagnosisPerDonorAndStudy").as("diagnosisGroup")
    val phenotypesPerDonorAndStudy = data("phenotypesPerDonorAndStudy").as("phenotypeGroup")
    val biospecimenWithSamples = data("biospecimenWithSamples").as("biospecimenWithSamples")
    val file = data("file").as("file")

    import spark.implicits._

    val fileDonors = file
      .join(donor, $"file.submitter_donor_id" === $"donor.submitter_donor_id")
      .groupBy($"file.study_id", $"file.file_name")
      .agg(
        collect_list(
          struct( cols =
            $"donor.*"
          )
        ) as "donors"
      ) as "fileWithDonors"

    val fileStudyJoin = file
      .join(studyDf, $"file.study_id" === $"study.study_id")
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

    fileStudyJoin
      .join(diagnosisPerDonorAndStudy, $"fileWithStudy.study_id" === $"diagnosisGroup.study_id" && $"fileWithStudy.submitter_donor_id" === $"diagnosisGroup.submitter_donor_id", "left")
      .join(phenotypesPerDonorAndStudy, $"fileWithStudy.study_id" === $"phenotypeGroup.study_id" && $"fileWithStudy.submitter_donor_id" === $"phenotypeGroup.submitter_donor_id", "left")
      .join(fileDonors, $"fileWithStudy.study_id" === $"fileWithDonors.study_id" && $"fileWithStudy.file_name" === $"fileWithDonors.file_name")
      .join(biospecimenWithSamples, $"fileWithStudy.submitter_biospecimen_id" === $"biospecimenWithSamples.submitter_biospecimen_id", "left")
      .select( cols =
        $"fileWithStudy.*",
        $"fileWithDonors.donors",
        $"mondo",
        $"biospecimenWithSamples.biospecimen" as "biospecimen",
        $"diagnosis_per_donor_per_study" as "diagnoses",
        $"observed_phenotype_tagged",
        $"not_observed_phenotype_tagged",
        $"observed_phenotypes",
        $"non_observed_phenotypes"
      )
      .drop($"submitter_donor_id")
      .drop($"submitter_biospecimen_id")
      .withColumn("dictionary_version", lit(studyNDF.dictionaryVersion))
      .withColumn("study_version", lit(studyNDF.studyVersion))
      .withColumn("study_version_creation_date", lit(studyNDF.studyVersionCreationDate))
  }
}

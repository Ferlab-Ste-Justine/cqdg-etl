package ca.cqdg.etl.rework.processes

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import ca.cqdg.etl.rework.models.{Metadata, NamedDataFrame}
import ca.cqdg.etl.rework.processes.ProcessETLUtils.columns._
import org.apache.spark.sql.functions.{array, collect_list, lit, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}

class FileIndex(studyDf: DataFrame, metadata: Metadata,
                inputData: Map[String, DataFrame])(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("files")

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    inputData
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val donor = data("donor").as("donor")
    val diagnosisPerDonorAndStudy = data("diagnosisPerDonorAndStudy").as("diagnosisGroup")
    val phenotypesPerStudyIdAndDonor = data("phenotypesPerStudyIdAndDonor").as("phenotypeGroup")
    val biospecimenWithSamples = data("biospecimenWithSamples").as("biospecimenWithSamples")
    val file = data("file").as("file")

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
      .join(phenotypesPerStudyIdAndDonor, $"fileWithStudy.study_id" === $"phenotypeGroup.study_id" && $"fileWithStudy.submitter_donor_id" === $"phenotypeGroup.submitter_donor_id", "left")
      .join(fileDonors, $"fileWithStudy.study_id" === $"fileWithDonors.study_id" && $"fileWithStudy.file_name" === $"fileWithDonors.file_name")
      .join(biospecimenWithSamples, $"fileWithStudy.submitter_biospecimen_id" === $"biospecimenWithSamples.submitter_biospecimen_id", "left")
      .select( cols =
        $"fileWithStudy.*",
        $"fileWithDonors.donors",
        $"mondo",
        $"biospecimenWithSamples.biospecimen" as "biospecimen",
        $"diagnoses",
        $"observed_phenotype_tagged",
        $"not_observed_phenotype_tagged",
        $"observed_phenotypes",
        $"non_observed_phenotypes",
      )
      .drop($"submitter_donor_id")
      .drop($"submitter_biospecimen_id")
      .drop($"file_name_keyword")
      .drop($"file_name_ngrams")
      .drop($"file_name")
      .withColumn("dictionary_version", lit(metadata.dictionaryVersion))
      .withColumn("study_version", lit(metadata.studyVersion))
      .withColumn("study_version_creation_date", lit(metadata.studyVersionCreationDate))
  }
}

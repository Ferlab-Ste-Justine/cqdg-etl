package ca.cqdg.etl

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period, ZoneId}
import ca.cqdg.etl.EtlUtils.columns.{ageAtRecruitment, isCancer, notNullCol, phenotypeObserved}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.{Random, Try}

package object EtlUtils {

  val DEFAULT_VALUE = "no-data"

  val dateFormats = Seq(
    new SimpleDateFormat("d/M/yyyy"),
    new SimpleDateFormat("d/MM/yyyy"),
    new SimpleDateFormat("dd/M/yyyy"),
    new SimpleDateFormat("dd/MM/yyyy")
  )

  val readOptions = Map(
    "inferSchema" -> "true",
    "comment" -> "#",
    "header"-> "true",
    "sep" -> "\t",
    "parserLib" -> "univocity",
    "quote" -> "\"",
    "mode" -> "FAILFAST",
    "nullValue" -> ""
  )

  def parseDate(date: String) : Option[LocalDate] = {
    dateFormats.toStream.map(formatter => {
      Try(formatter.parse(date).toInstant.atZone(ZoneId.systemDefault()).toLocalDate).toOption
    }).find(_.nonEmpty).flatten
  }

  def readCsvFile(filePaths: String)(implicit spark: SparkSession): DataFrame = {
    val inputs = filePaths.split(",")

    spark.read.format("csv")
      .options(readOptions)
      .load(inputs: _*)
  }

  def loadBiospecimens(biospecimenInputPath: String, sampleInputPath: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val biospecimen: DataFrame = readCsvFile(biospecimenInputPath)
    val samples: DataFrame = readCsvFile(sampleInputPath)

    val biospecimenWithRenamedColumns: DataFrame = biospecimen.
      select( cols =
        $"*",
        $"biospecimen_tissue_source" as "tissue_source",
        $"biospecimen_type" as "type",
        $"biospecimen_anatomic_location" as "anatomic_location",
        $"diagnosis_ICD_term" as "icd_term",
        $"diagnosis_ICD_term" as "icd_term_keyword"
      )
      .drop("biospecimen_tissue_source", "biospecimen_type", "biospecimen_anatomic_location", "diagnosis_ICD_term", "study_id", "submitter_donor_id")
      .withColumn("is_cancer", isCancer)
      .as("biospecimen")

    val samplesPerBiospecimen = biospecimenWithRenamedColumns.as("biospecimen")
      .join(samples.as("sample"), $"biospecimen.submitter_biospecimen_id" === $"sample.submitter_biospecimen_id")
      .groupBy($"sample.submitter_biospecimen_id")
      .agg(
        collect_list(
          struct(
            samples.columns.filterNot(List("study_id", "submitter_donor_id", "submitter_biospecimen_id").contains(_)).map(col) : _*)
        ) as "samples"
      ) as "samplesPerBiospecimen"

    val result = biospecimenWithRenamedColumns
      .join(samplesPerBiospecimen, $"biospecimen.submitter_biospecimen_id" === $"samplesPerBiospecimen.submitter_biospecimen_id", "left")
      .groupBy($"samplesPerBiospecimen.submitter_biospecimen_id")
      .agg(
        collect_list(
          struct($"biospecimen.*", $"samplesPerBiospecimen.samples")
        ) as "biospecimen"
      ) as "biospecimenWithSamples"

    //result.show(1, 0, true)
    result
  }

  def loadDonors(donorInputPath: String, familyRelationshipInputPath: String, familyHistoryInputPath: String, exposureInputPath: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val donor: DataFrame = readCsvFile(donorInputPath) as "donor"
    val familyRelationship: DataFrame = readCsvFile(familyRelationshipInputPath) as "familyRelationship"
    val familyHistory: DataFrame = readCsvFile(familyHistoryInputPath) as "familyHistory"
    val exposure: DataFrame = readCsvFile(exposureInputPath) as "exposure"

    val familyRelationshipsPerDonor = donor.as("donor")
      .join(familyRelationship.as("familyRelationship"), $"donor.submitter_family_id" === $"familyRelationship.submitter_family_id")
      .groupBy($"familyRelationship.submitter_family_id")
      .agg(
        collect_list(
          struct(familyRelationship.columns.filterNot(List("study_id", "submitter_family_id", "gender").contains(_)).map(col) : _*)
        ) as "familyRelationships"
      ) as "familyRelationshipsPerDonor"

    val familyHistoryPerDonor = donor.as("donor")
      .join(familyHistory.as("familyHistory"), $"donor.submitter_donor_id" === $"familyHistory.submitter_donor_id")
      .groupBy($"familyHistory.submitter_donor_id")
      .agg(
        collect_list(
          struct(familyHistory.columns.filterNot(List("study_id", "submitter_donor_id", "age TODAY").contains(_)).map(col) : _*)
        ) as "familyConditions"
      ) as "familyConditionsPerDonor"

    val exposurePerDonor = donor.as("donor")
      .join(exposure.as("exposure"), $"donor.submitter_donor_id" === $"exposure.submitter_donor_id")
      .groupBy($"exposure.submitter_donor_id")
      .agg(collect_list(
          struct(exposure.columns.filterNot(List("study_id", "submitter_donor_id").contains(_)).map(col) : _*)
        ) as "exposures"
      ) as "exposuresPerDonor"

    val result = donor
      .join(familyRelationshipsPerDonor, $"donor.submitter_family_id" === $"familyRelationshipsPerDonor.submitter_family_id", "left")
      .join(familyHistoryPerDonor, $"donor.submitter_donor_id" === $"familyConditionsPerDonor.submitter_donor_id", "left")
      .join(exposurePerDonor, $"donor.submitter_donor_id" === $"exposuresPerDonor.submitter_donor_id", "left")
      .drop($"exposuresPerDonor.submitter_donor_id")
      .drop($"familyConditionsPerDonor.submitter_donor_id")
      .drop($"familyRelationshipsPerDonor.submitter_family_id")
      .withColumn("gender", notNullCol($"gender"))
      .withColumn("ethnicity", notNullCol($"ethnicity"))
      .withColumn("age_at_recruitment", ageAtRecruitment)

    //result.show(1, 0, true)
    result
  }


  def loadDiagnoses(diagnoseInputPath: String, treatmentInputPath: String, followUpInputPath: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val diagnosis: DataFrame = readCsvFile(diagnoseInputPath) as "diagnosis"
    val treatment: DataFrame = readCsvFile(treatmentInputPath) as "treatment"
    val followUp: DataFrame = readCsvFile(followUpInputPath) as "follow_up"

    val treatmentRenamedColumns = Array($"treatment.diagnosis_ICD_term" as "icd_term", $"treatment.diagnosis_ICD_term" as "icd_term_keyword")
    val treatmentPerDiagnosis = diagnosis.as("diagnosis")
      .join(treatment.as("treatment"), $"diagnosis.submitter_diagnosis_id" === $"treatment.submitter_diagnosis_id")
      .groupBy($"treatment.submitter_diagnosis_id")
      .agg(
        collect_list(
          struct(
            treatmentRenamedColumns ++
              treatment.columns.filterNot(List("study_id", "submitter_donor_id", "submitter_diagnosis_id", "diagnosis_ICD_term").contains(_)).map(col) : _*)
        ) as "treatments"
      ) as "treatmentsPerDiagnosis"


    val followUpWithRenamedColumns = followUp
      .withColumn("is_cancer", isCancer)
      .withColumnRenamed("is_cancer", "is_cancer_follow_up")

    val followUpPerDiagnosis = diagnosis.as("diagnosis")
      .join(followUpWithRenamedColumns.as("follow_up"), $"diagnosis.submitter_diagnosis_id" === $"follow_up.submitter_diagnosis_id")
      .groupBy($"follow_up.submitter_diagnosis_id")
      .agg(
        collect_list(
          struct(
            followUpWithRenamedColumns.columns.filterNot(List("study_id", "submitter_donor_id", "submitter_diagnosis_id", "is_cancer").contains(_)).map(col) : _*)
        ) as "follow_ups"
      ) as "followUpsPerDiagnosis"


    val diagnosisWithTreatmentAndFollowUps = diagnosis
      .join(treatmentPerDiagnosis, $"diagnosis.submitter_diagnosis_id" === $"treatmentsPerDiagnosis.submitter_diagnosis_id", "left")
      .join(followUpPerDiagnosis, $"diagnosis.submitter_diagnosis_id" === $"followUpsPerDiagnosis.submitter_diagnosis_id", "left")
      .drop($"treatmentsPerDiagnosis.submitter_diagnosis_id")
      .drop($"followUpsPerDiagnosis.submitter_diagnosis_id")
      .select(cols =
        $"*",
        $"diagnosis.diagnosis_mondo_term" as "mondo_term",
        $"diagnosis.diagnosis_mondo_term" as "mondo_term_keyword",
        $"diagnosis.diagnosis_ICD_term" as "icd_term",
        $"diagnosis.diagnosis_ICD_term" as "icd_term_keyword",
        $"diagnosis.diagnosis_ICD_category" as "icd_category",
        $"diagnosis.diagnosis_ICD_category" as "icd_category_keyword"
      )

    val result = diagnosisWithTreatmentAndFollowUps.as("diagnosis")
      .groupBy($"diagnosis.submitter_donor_id", $"diagnosis.study_id")
      .agg(
        collect_list(
          struct(cols =
            diagnosisWithTreatmentAndFollowUps.columns.filterNot(List(
              "study_id",
              "submitter_donor_id",
              "diagnosis_ICD_term",
              "diagnosis_mondo_term",
              "diagnosis_ICD_category").contains(_)).map(col) : _*)
        ) as "diagnosis_per_donor_per_study"
      ) as "diagnosisGroup"

    //result.show(1, 0, true)
    result
  }

  def loadPhenotypes(diagnoseInputPath: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val phenotype: DataFrame = readCsvFile(diagnoseInputPath) as "phenotype"
    val phenotypeWithRenamedColumns = phenotype.select( cols =
      $"*",
      $"phenotype_HPO_code" as "hpo_code",
      $"phenotype_HPO_category" as "hpo_category",
      $"phenotype_HPO_category" as "hpo_category_keyword",
      $"phenotype_HPO_term" as "hpo_term",
      $"phenotype_HPO_term" as "hpo_term_keyword",
      phenotypeObserved
    ).drop("phenotype_observed", "phenotype_HPO_code", "phenotype_HPO_category", "phenotype_HPO_term")

    phenotypeWithRenamedColumns
      .where($"phenotype_observed_bool" === "true")
      .groupBy($"submitter_donor_id", $"study_id")
      .agg(
        collect_list(
          struct(phenotypeWithRenamedColumns.columns.filterNot(List("study_id", "submitter_donor_id", "phenotype_observed_bool").contains(_)).map(col) : _*)
        ) as "phenotypes_per_donor_per_study"
      ) as "phenotypeGroup"
  }

  object columns {
    val calculateAge: UserDefinedFunction = udf{ (dob: String, to: String) =>
      val dobDate = parseDate(dob)
      val toDate = parseDate(to)

      if (dobDate.isDefined && toDate.isDefined)
        Period.between(dobDate.get, toDate.get).getYears()
      else
        -1
    }

    def notNullCol(column: Column, defaultValue: String = DEFAULT_VALUE): Column = {
      when(column.isNotNull, column).otherwise(lit(defaultValue))
    }

    //TODO: Calculate the real file size - in mb
    val fileSize: Column = when(
                            col("file.file_name").isNotNull,
                            lit(1 + (24 * Random.nextDouble()))
    ) as "file_size"

    val phenotypeObserved: Column = when(
                                      col("phenotype_observed") isin ("YES", "Yes", "yes", "TRUE", "True", "true", "Y", "y", "1", 1),
                                      lit(true)).otherwise(lit(false)
                                    ) as "phenotype_observed_bool"

    val isCancer: Column = when(
                              col("is_cancer") isin ("YES", "Yes", "yes", "TRUE", "True", "true", "Y", "y", "1", 1),
                              lit(true)).otherwise(lit(false)
                            )

    val ageAtRecruitment: Column = when(
                                    col("dob").isNotNull && col("date_of_recruitement").isNotNull,
                                    //TODO: Figure out why the following is not working all the time.  Irregular date format? Try with a date padded with '0' -> dd/MM/yyyy
                                    //abs(floor(months_between(to_date(col("dob"), "d/M/yyyy"), to_date(col("date_of_recruitement"), "d/M/yyyy")) / 12))
                                    calculateAge(col("dob"), col("date_of_recruitement")))
                                   .otherwise(
                                    lit(DEFAULT_VALUE)
                                   ) as "age_at_recruitment"
  }
}



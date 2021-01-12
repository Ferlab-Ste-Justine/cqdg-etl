package ca.cqdg.etl

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period, ZoneId}

import ca.cqdg.etl.EtlUtils.columns.isCancer
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

    val biospecimen: DataFrame = readCsvFile(biospecimenInputPath) as "biospecimen"
    val samples: DataFrame = readCsvFile(sampleInputPath) as "samples"

    val samplesPerBiospecimen = biospecimen.as("biospecimen")
      .join(samples.as("sample"), $"biospecimen.submitter_biospecimen_id" === $"sample.submitter_biospecimen_id")
      .groupBy("sample.submitter_biospecimen_id")
      .agg(
        collect_list(
          struct( cols =
            $"submitter_sample_id",
            $"sample_type"
          )
        ) as "samples"
      )
      .as("samplesPerBiospecimen")

    val result = biospecimen
      .join(samplesPerBiospecimen, $"biospecimen.submitter_biospecimen_id" === $"samplesPerBiospecimen.submitter_biospecimen_id", "left")
      .groupBy("samplesPerBiospecimen.submitter_biospecimen_id")
      .agg(
        collect_list(
          struct( cols =
            $"biospecimen.submitter_biospecimen_id",
            $"tumor_normal_designation",
            $"biospecimen_tissue_source" as "tissue_source",
            $"biospecimen_type" as "type",
            isCancer,
            $"biospecimen_anatomic_location" as "anatomic_location",
            $"diagnosis_ICD_term" as "icd_term",
            $"diagnosis_ICD_term" as "icd_term_keyword",
            $"samples" as "samples"
          )
        ) as "biospecimen"
      )
      .as("biospecimenWithSamples")

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
      .groupBy("familyRelationship.submitter_family_id")
      .agg(
        collect_list(
          struct( cols =
            $"familyRelationship.submitter_family_id",
            $"submitter_donor_id_1",
            $"submitter_donor_id_2",
            $"family_type",
            $"familiy_1_2_relationship",
            $"family_2_1_relationship"
          )
        ) as "familyRelationships"
      )
      .as("familyRelationshipsPerDonor")

    val familyHistoryPerDonor = donor.as("donor")
      .join(familyHistory.as("familyHistory"), $"donor.submitter_donor_id" === $"familyHistory.submitter_donor_id")
      .groupBy("familyHistory.submitter_donor_id")
      .agg(
        collect_list(
          struct( cols =
            $"submitter_family_condition_id",
            $"family_condition_name",
            $"family_condition_age",
            $"family_condition_relationship",
            $"family_cancer_history"
          )
        ) as "familyConditions"
      )
      .as("familyConditionsPerDonor")

    val exposurePerDonor = donor.as("donor")
      .join(exposure.as("exposure"), $"donor.submitter_donor_id" === $"exposure.submitter_donor_id")
      .groupBy("exposure.submitter_donor_id")
      .agg(
        collect_list(
          struct( cols =
            $"submitter_exposure_id",
            $"smoking_status",
            $"alcohol_status",
            $"FSA"
          )
        ) as "exposures"
      )
      .as("exposuresPerDonor")

    val result = donor
      .join(familyRelationshipsPerDonor, $"donor.submitter_family_id" === $"familyRelationshipsPerDonor.submitter_family_id", "left")
      .join(familyHistoryPerDonor, $"donor.submitter_donor_id" === $"familyConditionsPerDonor.submitter_donor_id", "left")
      .join(exposurePerDonor, $"donor.submitter_donor_id" === $"exposuresPerDonor.submitter_donor_id", "left")
      .drop($"familyRelationshipsPerDonor.submitter_family_id")
      .drop($"familyConditionsPerDonor.submitter_donor_id")
      .drop($"exposuresPerDonor.submitter_donor_id")

    //result.show(1, 0, true)
    result
  }


  def loadDiagnoses(diagnoseInputPath: String, treatmentInputPath: String, followUpInputPath: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val diagnosis: DataFrame = readCsvFile(diagnoseInputPath) as "diagnosis"
    val treatment: DataFrame = readCsvFile(treatmentInputPath) as "treatment"
    val followUp: DataFrame = readCsvFile(followUpInputPath) as "followUp"

    val treatmentPerDiagnosis = diagnosis.as("diagnosis")
      .join(treatment.as("treatment"), $"diagnosis.submitter_diagnosis_id" === $"treatment.submitter_diagnosis_id")
      .groupBy("treatment.submitter_diagnosis_id")
      .agg(
        collect_list(
          struct( cols =
            $"submitter_treatment_id",
            $"treatment_type",
            $"treatment.diagnosis_ICD_term" as "icd_term",
            $"treatment.diagnosis_ICD_term"as "icd_term_keyword",
            $"treatment_is_primary",
            $"treatment_intent",
            $"treatment_response",
            $"medication_name",
            $"medication_code",
            $"medication_class",
            $"medication_start_date",
            $"medication_end_date"
          )
        ) as "treatments"
      )
      .as("treatmentsPerDiagnosis")

    val followUpPerDiagnosis = diagnosis.as("diagnosis")
      .join(followUp.as("followUp"), $"diagnosis.submitter_diagnosis_id" === $"followUp.submitter_diagnosis_id")
      .groupBy("followUp.submitter_diagnosis_id")
      .agg(
        collect_list(
          struct( cols =
            $"submitter_follow-up_id" as "submitter_follow_up_id", //NO HYPHENS IN COL NAMES - IT MAKES ARRANGER CRASH
            $"disease_status_at_followup",
            $"relapse_interval",
            $"days_to_follow_up"
          )
        ) as "followUps"
      )
      .as("followUpsPerDiagnosis")

    val diagnosisWithTreatmentAndFollowUps = diagnosis
      .join(treatmentPerDiagnosis, $"diagnosis.submitter_diagnosis_id" === $"treatmentsPerDiagnosis.submitter_diagnosis_id", "left")
      .join(followUpPerDiagnosis, $"diagnosis.submitter_diagnosis_id" === $"followUpsPerDiagnosis.submitter_diagnosis_id", "left")
      .drop($"treatmentsPerDiagnosis.submitter_diagnosis_id")
      .drop($"followUpsPerDiagnosis.submitter_diagnosis_id")

    val result = diagnosisWithTreatmentAndFollowUps.as("diagnosis")
      .groupBy("diagnosis.submitter_donor_id", "diagnosis.study_id")
      .agg(
        collect_list(
          struct(cols =
            $"diagnosis.diagnosis_mondo_term" as "mondo_term",
            $"diagnosis.diagnosis_mondo_term" as "mondo_term_keyword",
            $"diagnosis.diagnosis_ICD_term" as "icd_term",
            $"diagnosis.diagnosis_ICD_term" as "icd_term_keyword",
            $"diagnosis.diagnosis_ICD_category" as "icd_category",
            $"diagnosis.diagnosis_ICD_category" as "icd_category_keyword",
            $"diagnosis.age_at_diagnosis",
            $"treatments",
            $"followUps"
          )
        ) as "diagnosis_per_donor_per_study"
      ) as "diagnosisGroup"

    //result.show(1, 0, true)
    result
  }

  def loadPhenotypes(diagnoseInputPath: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val phenotype: DataFrame = readCsvFile(diagnoseInputPath) as "phenotype"

    phenotype
      .select( cols =
        $"submitter_donor_id",
        $"study_id",
        $"phenotype_HPO_code",
        $"phenotype_HPO_term",
        $"phenotype_HPO_category",
        columns.phenotypeObserved)
      .where($"phenotype_observed" === "true")
      .groupBy("submitter_donor_id", "study_id")
      .agg(
        collect_list(
          struct( cols =
            $"phenotype_HPO_code" as "hpo_code",
            $"phenotype_HPO_category" as "hpo_category",
            $"phenotype_HPO_category" as "hpo_category_keyword",
            $"phenotype_HPO_term" as "hpo_term",
            $"phenotype_HPO_term" as "hpo_term_keyword"
          )
        ) as "phenotypes_per_donor_per_study"
      )
      .as("phenotypeGroup")
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

    val fileId: Column = when(
                            col("file.study_id").isNotNull && col("file.file_name").isNotNull,
                            sha1(concat(col("file.study_id"), lit('_'), col("file.file_name")))
                          ) as "file_id"

    val donorId: Column = when(
      col("study.study_id").isNotNull && col("submitter_donor_id").isNotNull,
      sha1(concat(col("study.study_id"), lit('_'), col("submitter_donor_id")))
    ) as "donor_id"

    val phenotypeObserved: Column = when(
                                      col("phenotype_observed") isin ("YES", "Yes", "yes", "TRUE", "True", "true", "Y", "y", "1", 1),
                                      lit(true)).otherwise(lit(false)
                                    ) as "phenotype_observed"

    val isCancer: Column = when(
                              col("is_cancer") isin ("YES", "Yes", "yes", "TRUE", "True", "true", "Y", "y", "1", 1),
                              lit(true)).otherwise(lit(false)
                            ) as "is_cancer"

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



package bio.ferlab.cqdg.etl

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period, ZoneId}

import bio.ferlab.cqdg.etl.EtlUtils.columns.phenotypeObserved
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.{Try, Random}

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
    "sep" -> ",",
    "parserLib" -> "univocity",
    "quote" -> "\"",
    "mode" -> "FAILFAST",
    "nullValue" -> "-"
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

  def loadDiagnoses(diagnoseInputPath: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val diagnosis: DataFrame = readCsvFile(diagnoseInputPath) as "diagnosis"

    diagnosis
      .select( cols =
        $"submitter_donor_id",
        $"study_id",
        $"diagnosis_ICD_category",
        $"diagnosis_ICD_code",
        $"diagnosis_ICD_term",
        $"age_at_diagnosis")
      .groupBy("submitter_donor_id", "study_id")
      .agg(
        collect_list(
          struct(cols =
            $"diagnosis_ICD_code" as "icd_code",
            $"diagnosis_ICD_term" as "icd_term",
            $"diagnosis_ICD_term" as "icd_term_keyword",
            $"diagnosis_ICD_category" as "icd_category",
            $"diagnosis_ICD_category" as "icd_category_keyword",
            $"age_at_diagnosis"
          )
        ) as "diagnosis_per_donor_per_study"
      ) as "diagnosisGroup"
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
        phenotypeObserved)
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



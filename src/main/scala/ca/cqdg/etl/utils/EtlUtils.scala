package ca.cqdg.etl.utils

import ca.cqdg.etl.EtlApp.spark
import ca.cqdg.etl.model.NamedDataFrame
import ca.cqdg.etl.utils.EtlUtils.columns.{ageAtRecruitment, isCancer, notNullCol, phenotypeObserved}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.text.{Normalizer, SimpleDateFormat}
import java.time.{LocalDate, Period, ZoneId}
import scala.util.{Properties, Random, Try}

object EtlUtils {

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
    "header" -> "true",
    "sep" -> "\t",
    "parserLib" -> "univocity",
    "quote" -> "\"",
    "mode" -> "PERMISSIVE",
    "nullValue" -> ""
  )

  val writeOptions = Map(
    "comment" -> "#",
    "header" -> "true",
    "sep" -> "\t",
    "quote" -> "\"",
    "mode" -> "PERMISSIVE",
    "nullValue" -> ""
  )

  def getConfiguration(key: String, default: String): String = {
    Properties.envOrElse(key, Properties.propOrElse(key, default))
  }

  def parseDate(date: String): Option[LocalDate] = {
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

  def sanitize(str: String): String = {
    val noExtension = if (str.indexOf(".") > -1) str.substring(0, str.indexOf(".")) else str;
    val normalized = Normalizer.normalize(noExtension, Normalizer.Form.NFD)
    val noSpecialChars = normalized.replaceAll("[^a-zA-Z]", "")
    noSpecialChars.toLowerCase().trim()
  }

  def getDataframe(name: String, dfList: List[NamedDataFrame]): NamedDataFrame = {
    dfList.find(df => df.name == name).getOrElse(throw new RuntimeException(s"Could find any dataframe named ${name}"))
  }

  def loadAll(dfList: List[NamedDataFrame])(ontologies: Map[String, DataFrame])
             (implicit spark: SparkSession): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    val donorsNDF = getDataframe("donor", dfList)
    val familyRelationshipNDF = getDataframe("familyrelationship", dfList)
    val familyHistoryNDF = getDataframe("familyhistory", dfList)
    val exposureNDF = getDataframe("exposure", dfList)
    val diagnosisNDF = getDataframe("diagnosis", dfList)
    val treatmentNDF = getDataframe("treatment", dfList)
    val followUpNDF = getDataframe("followup", dfList)
    val phenotypeNDF = getDataframe("phenotype", dfList)
    val fileNDF = getDataframe("file", dfList)
    val biospecimenNDF = getDataframe("biospecimen", dfList)
    val sampleNDF = getDataframe("sampleregistration", dfList)

    val donor: DataFrame = loadDonors(donorsNDF.dataFrame as "donor",
      familyRelationshipNDF.dataFrame as "familyRelationship",
      familyHistoryNDF.dataFrame as "familyHistory",
      exposureNDF.dataFrame as "exposure") as "donor"

    val diagnosisPerDonorAndStudy: DataFrame = loadDiagnoses(diagnosisNDF.dataFrame as "diagnosis",
      treatmentNDF.dataFrame as "treatment",
      followUpNDF.dataFrame as "follow_up")

    val phenotypesPerStudyIdAndDonor = addAncestorsToTerm("phenotype_HPO_code")(phenotypeNDF.dataFrame, ontologies.head._2)

    val treatmentsPerDonorAndStudy: DataFrame = loadTreatments(treatmentNDF.dataFrame as "treatment")

    val biospecimenWithSamples: DataFrame = loadBiospecimens(biospecimenNDF.dataFrame, sampleNDF.dataFrame) as "biospecimenWithSamples"
    val file: DataFrame = fileNDF.dataFrame as "file"

    (donor, diagnosisPerDonorAndStudy, phenotypesPerStudyIdAndDonor, biospecimenWithSamples, file, treatmentsPerDonorAndStudy)
  }

  def addAncestorsToTerm(dataColName: String)(dataDf: DataFrame, termsDf: DataFrame)
                        (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val phenotypes_with_ancestors = dataDf.join(termsDf, dataDf(dataColName) === termsDf("id"), "left_outer")

    val taggedPhenotypes =
      phenotypes_with_ancestors
        .select(cols=
          $"study_id",
          $"submitter_donor_id",
          $"id" ,
          $"name",
          $"parents",
          $"age_at_phenotype",
          phenotypeObserved,
          $"is_leaf"
        )
        .withColumn("is_tagged", lit(true))
        .filter(phenotypes_with_ancestors.col("id").isNotNull)

    val ancestors_exploded =
      phenotypes_with_ancestors
        .select(cols=
          $"study_id",
          $"submitter_donor_id",
          $"age_at_phenotype",
          $"phenotype_observed",
          explode_outer($"ancestors") as "ancestors_exploded"
        )
        .withColumn("is_leaf", lit(false))

    val parentsPhenotypes = ancestors_exploded.select(cols=
      $"study_id",
      $"submitter_donor_id",
      $"ancestors_exploded.id" as "id",
      $"ancestors_exploded.name" as "name",
      $"ancestors_exploded.parents" as "parents",
      $"age_at_phenotype",
      phenotypeObserved
    )
      .withColumn("is_leaf", lit(false))
      .withColumn("is_tagged", lit(false))
      .filter(phenotypes_with_ancestors.col("id").isNotNull)

    val combinedDF: DataFrame = taggedPhenotypes.union(parentsPhenotypes)
    val combinedPhenotypes = combinedDF
      .groupBy($"study_id", $"submitter_donor_id")
      .agg(collect_list(
        struct(cols =
          $"id".as("phenotype_id"),
          $"name",
          $"parents",
          $"age_at_phenotype",
          $"phenotype_observed_bool",
          $"is_leaf",
          $"is_tagged"
        )
      ) as "phenotypes") as "phenotypeGroup"

    combinedPhenotypes
  }

  def broadcastStudies(listNamedDF: List[NamedDataFrame])(implicit spark: SparkSession): Broadcast[DataFrame] = {
    import spark.implicits._

    val study: DataFrame = getDataframe("study", listNamedDF).dataFrame
      .select( cols=
        $"*",
        $"study_id" as "study_id_keyword",
        $"short_name" as "short_name_keyword",
        $"short_name" as "short_name_ngrams"
      )
      .as("study")

    spark.sparkContext.broadcast(study)
  }

  def loadBiospecimens(biospecimen: DataFrame, samples: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val biospecimenWithRenamedColumns: DataFrame = biospecimen
      .select(cols =
        $"*",
        $"biospecimen_tissue_source" as "tissue_source",
        $"biospecimen_type" as "type",
        $"biospecimen_anatomic_location" as "anatomic_location"
        // TODO: We need to load icd_term and icd_term keyword by looking up the description from ICD dictionary
        // TODO: See notes & scripts here: https://docs.qa.cqdg.ferlab.bio/dictionary/
        /*$"diagnosis_ICD_term" as "icd_term",
        $"diagnosis_ICD_term" as "icd_term_keyword"*/
      )
      .drop("biospecimen_tissue_source", "biospecimen_type", "biospecimen_anatomic_location", "study_id", "submitter_donor_id")
      .as("biospecimen")

    val samplesPerBiospecimen = biospecimenWithRenamedColumns.as("biospecimen")
      .join(samples.as("sample"), Seq("submitter_biospecimen_id"))
      .groupBy("submitter_biospecimen_id")
      .agg(
        collect_list(
          struct(
            samples.columns.filterNot(List("study_id", "submitter_donor_id", "submitter_biospecimen_id").contains(_)).map(col): _*)
        ) as "samples"
      ) as "samplesPerBiospecimen"

    val result = biospecimenWithRenamedColumns
      .join(samplesPerBiospecimen,  Seq("submitter_biospecimen_id"))
      .groupBy("submitter_biospecimen_id")
      .agg(
        collect_list(
          struct($"biospecimen.*", $"samplesPerBiospecimen.samples")
        ) as "biospecimen"
      ) as "biospecimenWithSamples"

    //result.show(1, 0, true)
    result
  }

  def loadDonors(donor: DataFrame, familyRelationship: DataFrame, familyHistory: DataFrame, exposure: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val familyRelationshipsPerDonor = donor.as("donor")
      .join(
        familyRelationship.as("familyRelationship"),
        $"donor.submitter_donor_id" === $"familyRelationship.submitter_donor_id_1" || $"donor.submitter_donor_id" === $"familyRelationship.submitter_donor_id_2", "left")
      .withColumn("joinCol", when($"donor.submitter_donor_id" === $"familyRelationship.submitter_donor_id_1", col("submitter_donor_id_1")).otherwise(col("submitter_donor_id_2")))
      .groupBy($"joinCol")
      .agg(
        collect_list(
          struct(familyRelationship.columns.filterNot(List("study_id", "gender").contains(_)).map(col): _*)
        ) as "familyRelationships"
      ) as "familyRelationshipsPerDonor"

    val familyHistoryPerDonor = donor.as("donor")
      .join(familyHistory.as("familyHistory"), $"donor.submitter_donor_id" === $"familyHistory.submitter_donor_id")
      .groupBy($"familyHistory.submitter_donor_id")
      .agg(
        collect_list(
          struct(familyHistory.columns.filterNot(List("study_id", "submitter_donor_id", "age TODAY").contains(_)).map(col): _*)
        ) as "familyConditions"
      ) as "familyConditionsPerDonor"

    val exposurePerDonor = donor.as("donor")
      .join(exposure.as("exposure"), $"donor.submitter_donor_id" === $"exposure.submitter_donor_id")
      .groupBy($"exposure.submitter_donor_id")
      .agg(collect_list(
        struct(exposure.columns.filterNot(List("study_id", "submitter_donor_id").contains(_)).map(col): _*)
      ) as "exposures"
      ) as "exposuresPerDonor"

    val result = donor
      .join(familyRelationshipsPerDonor, donor("submitter_donor_id") === $"familyRelationshipsPerDonor.joinCol", "left")
      .join(familyHistoryPerDonor, donor("submitter_donor_id") === $"familyConditionsPerDonor.submitter_donor_id", "left")
      .join(exposurePerDonor, donor("submitter_donor_id") === $"exposuresPerDonor.submitter_donor_id", "left")
      .drop($"exposuresPerDonor.submitter_donor_id")
      .drop($"familyConditionsPerDonor.submitter_donor_id")
      .drop($"familyRelationshipsPerDonor.submitter_family_id")
      .drop("joinCol")
      .withColumn("gender", notNullCol($"gender"))
      .withColumn("ethnicity", notNullCol($"ethnicity"))
      .withColumn("age_at_recruitment", ageAtRecruitment)


    //result.printSchema();
    //result.show(1, 0, true)
    result
  }


  def loadDiagnoses(diagnosis: DataFrame, treatment: DataFrame, followUp: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // TODO: Replace empty array by icd_term loaded from ICD based on diagnosis_ICD_code
    // val treatmentRenamedColumns = Array($"treatment.diagnosis_ICD_term" as "icd_term", $"treatment.diagnosis_ICD_term" as "icd_term_keyword")
    val treatmentPerDiagnosis = diagnosis.as("diagnosis")
      .join(treatment.as("treatment"), Seq("submitter_diagnosis_id"))
      .groupBy("submitter_diagnosis_id")
      .agg(
        collect_list(
          struct(
            // treatmentRenamedColumns ++
            treatment.columns.filterNot(List("study_id", "submitter_donor_id", "submitter_diagnosis_id").contains(_)).map(col): _*)
        ) as "treatments"
      ) as "treatmentsPerDiagnosis"


    val followUpPerDiagnosis = diagnosis.as("diagnosis")
      .join(followUp, Seq("submitter_diagnosis_id"))
      .groupBy("submitter_diagnosis_id")
      .agg(
        collect_list(
          struct(
            followUp.columns.filterNot(List("study_id", "submitter_donor_id", "submitter_diagnosis_id").contains(_)).map(col): _*)
        ) as "follow_ups"
      ) as "followUpsPerDiagnosis"


    val diagnosisWithTreatmentAndFollowUps = diagnosis
      .join(treatmentPerDiagnosis, Seq("submitter_diagnosis_id"), "left")
      .join(followUpPerDiagnosis, Seq("submitter_diagnosis_id"), "left")
      .withColumn("is_cancer", isCancer)
    // TODO: load the following based on their respective code.
    /*.select(cols =
        $"*",
        $"diagnosis.diagnosis_mondo_term" as "mondo_term",
        $"diagnosis.diagnosis_mondo_term" as "mondo_term_keyword",
        $"diagnosis.diagnosis_ICD_term" as "icd_term",
        $"diagnosis.diagnosis_ICD_term" as "icd_term_keyword",
        $"diagnosis.diagnosis_ICD_category" as "icd_category",
        $"diagnosis.diagnosis_ICD_category" as "icd_category_keyword"
      )*/

    val result = diagnosisWithTreatmentAndFollowUps.as("diagnosis")
      .groupBy($"diagnosis.submitter_donor_id", $"diagnosis.study_id")
      .agg(
        collect_list(
          struct(cols =
            diagnosisWithTreatmentAndFollowUps.columns.filterNot(List(
              "study_id",
              "submitter_donor_id").contains(_)).map(col): _*)
        ) as "diagnosis_per_donor_per_study"
      ) as "diagnosisGroup"

    //result.show(1, 0, true)
    result
  }

  def loadPhenotypes(phenotype: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val phenotypeWithRenamedColumns = phenotype.select(cols =
      $"*",
      $"phenotype_HPO_code" as "hpo_code",
      // TODO: Derive the following from phenotype_HPO_code
      /*
      $"phenotype_HPO_category" as "hpo_category",
      $"phenotype_HPO_category" as "hpo_category_keyword",
      $"phenotype_HPO_term" as "hpo_term",
      $"phenotype_HPO_term" as "hpo_term_keyword",*/
      phenotypeObserved
    ).drop("phenotype_observed", "phenotype_HPO_code")

    phenotypeWithRenamedColumns
      .where($"phenotype_observed_bool" === "true")
      .groupBy($"submitter_donor_id", $"study_id")
      .agg(
        collect_list(
          struct(phenotypeWithRenamedColumns.columns.filterNot(List("study_id", "submitter_donor_id", "phenotype_observed_bool").contains(_)).map(col): _*)
        ) as "phenotypes_per_donor_per_study"
      ) as "phenotypeGroup"
  }

  def loadTreatments(treatment: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    treatment
      .groupBy($"submitter_donor_id", $"study_id")
      .agg(
        collect_list(
          struct(treatment.columns.filterNot(List("study_id", "submitter_donor_id").contains(_)).map(col): _*)
        ) as "treatments_per_donor_per_study"
      ) as "treatmentGroup"

  }

  object columns {
    val calculateAge: UserDefinedFunction = udf { (dob: String, to: String) =>
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
      col("phenotype_observed") isin("YES", "Yes", "yes", "TRUE", "True", "true", "Y", "y", "1", 1),
      lit(true)).otherwise(lit(false)
    ) as "phenotype_observed_bool"

    val isCancer: Column =
      when(
        col("is_cancer").isin("YES", "Yes", "yes", "TRUE", "True", "true", "Y", "y", "1", 1), lit(true)
      ).otherwise(lit(false))

    val ageAtRecruitment: Column = when(
      col("dob").isNotNull && col("date_of_recruitment").isNotNull,
      //TODO: Figure out why the following is not working all the time.  Irregular date format? Try with a date padded with '0' -> dd/MM/yyyy
      //abs(floor(months_between(to_date(col("dob"), "d/M/yyyy"), to_date(col("date_of_recruitment"), "d/M/yyyy")) / 12))
      calculateAge(col("dob"), col("date_of_recruitment")))
      .otherwise(
        lit(DEFAULT_VALUE)
      ) as "age_at_recruitment"
  }

}

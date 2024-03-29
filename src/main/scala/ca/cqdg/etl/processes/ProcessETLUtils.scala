package ca.cqdg.etl.processes

import ca.cqdg.etl.EtlUtils.parseDate
import ca.cqdg.etl.models.NamedDataFrame
import ca.cqdg.etl.processes.ProcessETLUtils.columns.{ageAtRecruitment, notNullCol}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.Period
import scala.util.Random

object ProcessETLUtils {

  def getDataframe(name: String, dfList: List[NamedDataFrame]): NamedDataFrame = {
    dfList.find(df => df.name == name).getOrElse(throw new RuntimeException(s"Could find any dataframe named ${name}"))
  }

  def loadAll(dfList: List[NamedDataFrame])(ontologies: Map[String, DataFrame])
             (implicit spark: SparkSession): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    import spark.implicits._

    val donorsNDF = getDataframe("donor", dfList)
    val familyRelationshipNDF = getDataframe("family", dfList)
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

    val phenotypeNDFCleanObserved = phenotypeNDF.dataFrame
      .withColumnRenamed("age_at_phenotype", "age_at_event")
      .select(cols = $"*", phenotypeObserved)
      .drop($"phenotype_observed")
      .as("phenotypesClean")

    val phenotypesObservedPerStudyIdAndDonor =
      addAncestorsToTerm("phenotype_HPO_code", "observed_phenotypes", "internal_phenotype_id")(
        phenotypeNDFCleanObserved.filter($"phenotype_observed_bool" === true),
        ontologies("hpo")
      )

    val phenotypesNotObservedPerStudyIdAndDonor =
      addAncestorsToTerm("phenotype_HPO_code", "non_observed_phenotypes", "internal_phenotype_id")(
        phenotypeNDFCleanObserved.filter($"phenotype_observed_bool" === false),
        ontologies("hpo")
      )

    val taggedObservedPhenotypes = phenotypesObservedPerStudyIdAndDonor._2
      .withColumnRenamed("id", "phenotype_id" )
      .as("observed_phenotypes_tagged")

    val taggedNotObservedPhenotypes = phenotypesNotObservedPerStudyIdAndDonor._2
      .withColumnRenamed("id", "phenotype_id" )
      .as("not_observed_phenotypes_tagged")

    val taggedObservedPhenotypesGrouped = taggedObservedPhenotypes
      .groupBy($"study_id", $"submitter_donor_id")
      .agg(
        collect_list(
          struct($"phenotype_id",
            $"name",
            $"parents",
            $"display_name",
            $"main_category",
            $"is_leaf",
            $"is_tagged",
            $"internal_phenotype_id",
            array($"age_at_event").as("age_at_event"),
          )
        ).as("observed_phenotype_tagged")
      )

    val taggedNotObservedPhenotypesGrouped = taggedNotObservedPhenotypes
      .groupBy($"study_id", $"submitter_donor_id")
      .agg(
        collect_list(
          struct($"phenotype_id",
            $"name",
            $"parents",
            $"display_name",
            $"is_leaf",
            $"is_tagged",
            $"internal_phenotype_id",
            array($"age_at_event").as("age_at_event"),
            $"main_category"
          )
        ).as("not_observed_phenotype_tagged")
      )

    val phenotypesPerStudyIdAndDonor = phenotypeNDFCleanObserved
      .select($"study_id", $"submitter_donor_id").distinct()
      .join(taggedObservedPhenotypesGrouped, Seq("study_id", "submitter_donor_id"), "left")
      .join(taggedNotObservedPhenotypesGrouped, Seq("study_id", "submitter_donor_id"), "left")
      .join(phenotypesObservedPerStudyIdAndDonor._1, Seq("study_id", "submitter_donor_id"), "left")
      .join(phenotypesNotObservedPerStudyIdAndDonor._1, Seq("study_id", "submitter_donor_id"), "left")

    val mondoPerStudyIdAndDonor =
      addAncestorsToTerm("diagnosis_mondo_code", "mondo", "internal_diagnosis_id")(
        diagnosisNDF.dataFrame
          .withColumnRenamed("age_at_diagnosis", "age_at_event"),
        ontologies("mondo"))

    val regexIcd = "^(.*)\\|([1-9]*)"

    val splitIcd = ontologies("icd")
      .withColumn("id_extract", regexp_extract($"id", regexIcd, 1))
      .withColumn("chapter", regexp_extract($"id", regexIcd, 2))
      .drop("id")
      .withColumnRenamed("id_extract", "id")

    val icdPerStudyIdAndDonor =
      addAncestorsToTerm("diagnosis_ICD_code", "icd", "internal_diagnosis_id")(
        diagnosisNDF.dataFrame
          .withColumnRenamed("age_at_diagnosis", "age_at_event"),
        splitIcd)

    val diagnosisWithMondoICDTagged = diagnosisNDF.dataFrame.as("diagnosis")
      .join(mondoPerStudyIdAndDonor._2.as("diagnosis_mondo_tagged"),
        Seq("study_id", "submitter_donor_id", "submitter_diagnosis_id"),
        "left")
      .select(
        $"diagnosis.*",
        struct(
          $"id" as ("phenotype_id"),
          $"name",
          $"parents",
          $"display_name",
          $"main_category",
          array($"age_at_event").as("age_at_event"),
          $"is_leaf",
          $"is_tagged",
          $"diagnosis.internal_diagnosis_id"
        ).as("tagged_mondo")
      )
      .join(icdPerStudyIdAndDonor._2.as("diagnosis_icd_tagged"),
        Seq("study_id", "submitter_donor_id", "submitter_diagnosis_id"),
        "left")
      .select(
        $"diagnosis.*",
        $"tagged_mondo",
        struct(
          $"id" as ("phenotype_id"),
          $"name",
          $"parents",
          $"display_name",
          $"main_category",
          array($"age_at_event").as("age_at_event"),
          $"is_leaf",
          $"is_tagged",
          $"diagnosis.internal_diagnosis_id"
        ).as("tagged_icd")
      )

    val diagnosisPerDonorAndStudy: DataFrame =
      loadDiagnoses(diagnosisWithMondoICDTagged as "diagnosis",
        treatmentNDF.dataFrame as "treatment",
        followUpNDF.dataFrame as "follow_up")
        .join(mondoPerStudyIdAndDonor._1.as("mondo"),
          Seq("study_id", "submitter_donor_id"),"left")
        .join(icdPerStudyIdAndDonor._1.as("icd"),
          Seq("study_id", "submitter_donor_id"),"left")

    val treatmentsPerDonorAndStudy: DataFrame = loadPerDonorAndStudy(treatmentNDF.dataFrame, "treatment")
    val exposuresPerDonorAndStudy: DataFrame = loadPerDonorAndStudy(exposureNDF.dataFrame, "exposure")
    val followUpsPerDonorAndStudy: DataFrame = loadPerDonorAndStudy(followUpNDF.dataFrame, "followUp")
    val familyHistoryPerDonorAndStudy: DataFrame = loadPerDonorAndStudy(familyHistoryNDF.dataFrame, "familyHistory")
    val familyRelationshipPerDonorAndStudy: DataFrame = loadPerDonorAndStudy(familyRelationshipNDF.dataFrame, "familyRelationship")

    val biospecimenWithSamples: DataFrame = loadBiospecimens(biospecimenNDF.dataFrame, sampleNDF.dataFrame) as "biospecimenWithSamples"
    val file: DataFrame = fileNDF.dataFrame as "file"

    (donor, diagnosisPerDonorAndStudy, phenotypesPerStudyIdAndDonor, biospecimenWithSamples, file, treatmentsPerDonorAndStudy, exposuresPerDonorAndStudy, followUpsPerDonorAndStudy, familyHistoryPerDonorAndStudy, familyRelationshipPerDonorAndStudy)
  }


  def loadDonors(donor: DataFrame, familyRelationship: DataFrame, familyHistory: DataFrame, exposure: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val familyRelationshipsPerDonor = donor.as("donor")
      .join(
        familyRelationship.as("familyRelationship"), Seq("submitter_donor_id"), "left")
      .withColumn("joinCol", $"submitter_donor_id")
      .groupBy($"joinCol")
      .agg(
        collect_list(
          struct(familyRelationship.columns.filterNot(List("study_id", "gender", "is_a_proband").contains(_)).map(col): _*)
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

    result
  }

  def addAncestorsToTerm(dataColName: String, ontologyTermName: String, internalIdColumnName:String)(dataDf: DataFrame, termsDf: DataFrame)
                        (implicit spark: SparkSession): (DataFrame, DataFrame) = {
    import spark.implicits._


    val phenotypes_with_ancestors = dataDf.join(termsDf, dataDf(dataColName) === termsDf("id"), "left_outer")

    val (condition: Column, _type: String) = dataColName match {
      case "phenotype_HPO_code" =>
        (array_contains(col("main_category.parents"), "Phenotypic abnormality (HP:0000118)"), "phenotype")
      case "diagnosis_mondo_code" =>
        (array_contains(col("main_category.parents"), "disease or disorder (MONDO:0000001)"), "diagnosis")
      //"diagnosis_ICD_code"
      case _ =>
        ($"main_category.id".rlike("^[A-Z][0-9]{2}-[A-Z][0-9]{2}"), "diagnosis")
    }
    val main_categoryDf = extractMainCategory(phenotypes_with_ancestors, dataColName, condition, _type)

    val taggedPhenotypes =
      phenotypes_with_ancestors
        .select(cols=
          $"study_id",
          col(s"submitter_${_type}_id"),
          $"submitter_donor_id",
          $"id" ,
          $"name",
          concat(col("name"), lit(" ("), col("id"), lit(")")).alias("display_name"),
          $"parents",
          $"age_at_event",
          col(internalIdColumnName),
          $"is_leaf"
        )
        .withColumn("is_tagged", lit(true))
        .filter(phenotypes_with_ancestors.col("id").isNotNull)

    val ancestors_exploded =
      phenotypes_with_ancestors
        .select(cols=
          $"study_id",
          $"submitter_donor_id",
          $"age_at_event",
          col(internalIdColumnName),
          explode_outer($"ancestors") as "ancestors_exploded",
        )
        .withColumn("is_leaf", lit(false))

    val parentsPhenotypes = ancestors_exploded.select(cols=
      $"study_id",
      $"submitter_donor_id",
      $"ancestors_exploded.id" as "id",
      $"ancestors_exploded.name" as "name",
      concat(
        col("ancestors_exploded.name"),
        lit(" ("),
        col("ancestors_exploded.id"),
        lit(")")).alias("display_name"),
      $"ancestors_exploded.parents" as "parents",
      $"age_at_event",
      col(internalIdColumnName)
    )
      .withColumn("is_leaf", lit(false))
      .withColumn("is_tagged", lit(false))
      .filter(phenotypes_with_ancestors.col("id").isNotNull)

    val combinedDF: DataFrame = taggedPhenotypes.drop(col(s"submitter_${_type}_id")).union(parentsPhenotypes)

    val groupAgesAtPhenotype = combinedDF
      .groupBy(
        $"study_id",
        $"submitter_donor_id",
        $"id",
        $"name",
        $"display_name",
        $"parents",
        $"is_leaf",
        $"is_tagged",
        col(internalIdColumnName),
      )
      .agg(collect_list(
        array(cols = $"age_at_event")
      ) as "age_at_event_raw")
      .select(
        $"*",
        array_distinct(sort_array(flatten($"age_at_event_raw"))) as "age_at_event"
      )
      .drop($"age_at_event_raw")

    val combinedPhenotypes = groupAgesAtPhenotype
      .groupBy($"study_id", $"submitter_donor_id")
      .agg(collect_list(
        struct(cols =
          $"id".as("phenotype_id"),
          $"name",
          $"display_name",
          $"parents",
          $"age_at_event",
          col(internalIdColumnName),
          $"is_leaf",
          $"is_tagged"
        )
      ) as s"$ontologyTermName") as "phenotypeGroup"

    val tagged_with_main_category = taggedPhenotypes
      .join(main_categoryDf, Seq("study_id", "submitter_donor_id", s"submitter_${_type}_id", "id"), "left")
      .drop($"internal_diagnosis_id")

    (combinedPhenotypes, tagged_with_main_category)
  }

  val phenotypeObserved: Column = when(
    col("phenotype_observed").cast(StringType).isin("YES", "Yes", "yes", "TRUE", "True", "true", "Y", "y", "1", 1),
    lit(true)).otherwise(lit(false)
  ) as "phenotype_observed_bool"

  def loadDiagnoses(
                     diagnosis: DataFrame,
                     treatment: DataFrame,
                     followUp: DataFrame)(implicit spark: SparkSession): DataFrame = {
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
    //      .withColumn("is_cancer", isCancer) //FIXME is this necessary?
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
        ) as "diagnoses"
      ) as "diagnosisGroup"

    result
  }

  private def extractMainCategory(
                                   ontology_with_ancestors: DataFrame,
                                   dataColName: String,
                                   whereCondition: Column,
                                   _type: String)(implicit spark: SparkSession) = {
    import spark.implicits._

    ontology_with_ancestors
      .select($"study_id",
        $"submitter_donor_id",
        col(s"submitter_${_type}_id"),
        col(dataColName) as "id",
        explode_outer($"ancestors") as "main_category"
      )
      .where(whereCondition)
      .groupBy($"study_id", $"submitter_donor_id", col(s"submitter_${_type}_id"), $"id")
      .agg(
        concat(first($"main_category.name"), lit(" ("), first($"main_category.id"), lit(")"))
          .as("main_category"))
  }

  def loadPerDonorAndStudy(dataFrame: DataFrame, namedAs: String, submitterDonorIdColName: String = "submitter_donor_id")(implicit spark: SparkSession): DataFrame = {

    dataFrame
      .groupBy("study_id",submitterDonorIdColName)
      .agg(
        collect_list(
          struct(dataFrame.columns.filterNot(List("study_id", submitterDonorIdColName).contains(_)).map(col): _*)
        ).as(s"${namedAs}s_per_donor_per_study")
      ).as(namedAs)
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

    result
  }

  object columns {

    val DEFAULT_VALUE = "no-data"

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

    def isNotBlank(col: Column): Column = {
      col.isNotNull && trim(col) =!= ""
    }

    def toBoolean(col: Column): Column = when(
      col.geq(1),lit(true)
    ) otherwise( lit(false))

    //TODO: Calculate the real file size - in mb
    val fileSize: Column = when(
      col("file.file_name").isNotNull,
      lit(1 + (24 * Random.nextDouble()))
    ) as "file_size"

    val phenotypeObserved: Column = when(
      col("phenotype_observed").cast(StringType).isin("YES", "Yes", "yes", "TRUE", "True", "true", "Y", "y", "1", 1),
      lit(true)).otherwise(lit(false)
    ) as "phenotype_observed_bool"

    val isCancer: Column =
      when(
        col("is_cancer").cast(StringType).isin("YES", "Yes", "yes", "TRUE", "True", "true", "Y", "y", "1", 1), lit(true)
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

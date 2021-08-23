package ca.cqdg.etl.utils

import ca.cqdg.etl.processes.ProcessETLUtils.{addAncestorsToTerm, loadBiospecimens, loadPerDonorAndStudy}
import ca.cqdg.etl.testutils.WithSparkSession
import ca.cqdg.etl.testutils.model._
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EtlUtilsSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  "loadBiospecimens" should "transform data in expected format" in {
    val sampleRegistrationDf = Seq(SampleRegistrationInput()).toDF()
    val biospecimenDf = Seq(BiospecimenInput()).toDF()
      val result = loadBiospecimens(biospecimenDf, sampleRegistrationDf)

    result.as[BiospecimenOutput].collect().head shouldBe BiospecimenOutput()
  }

  "loadTreatments" should "transform data in expected format" in {
    val treatmentDf = Seq(TreatmentInput()).toDF()
    val result = loadPerDonorAndStudy(treatmentDf, "treatment")

    result.as[TreatmentOutput].collect().head shouldBe TreatmentOutput()
  }

  "addAncestorsToTerm" should "transform data in expected format" in {

    val phenotypesDf =
      Seq(PhenotypeInput(`phenotype_HPO_code` = "HP:0001694")).toDF
      .withColumnRenamed("age_at_phenotype", "age_at_event")

    val hpoDf = Seq(HpoTermsInput()).toDF()

    val result = addAncestorsToTerm("phenotype_HPO_code", "phenotypes", "internal_phenotype_id")(phenotypesDf, hpoDf)._1

    result.as[PhenotypeWithHpoOutput].collect().head shouldBe PhenotypeWithHpoOutput()
  }


}

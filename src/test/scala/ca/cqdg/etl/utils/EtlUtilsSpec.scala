package ca.cqdg.etl.utils

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
      val result = EtlUtils.loadBiospecimens(biospecimenDf, sampleRegistrationDf)

    result.as[BiospecimenOutput].collect().head shouldBe BiospecimenOutput()
  }


  "loadPhenotypes" should "transform data in expected format" in {
    val phenotypesDf = Seq(PhenotypeInput()).toDF()
    val result = EtlUtils.loadPhenotypes(phenotypesDf)

    result.as[PhenotypeOutput].collect().head shouldBe PhenotypeOutput()
  }


  "loadTreatments" should "transform data in expected format" in {
    val treatmentDf = Seq(TreatmentInput()).toDF()
    val result = EtlUtils.loadTreatments(treatmentDf)

    result.as[TreatmentOutput].collect().head shouldBe TreatmentOutput()
  }

  "addAncestorsToTerm" should "transform data in expected format" in {

    val phenotypesDf = Seq(PhenotypeInput(`phenotype_HPO_code` = "HP:0001694")).toDF

    val hpoDf = Seq(HpoTermsInput()).toDF()

    val result = EtlUtils.addAncestorsToTerm("phenotype_HPO_code")(phenotypesDf, hpoDf)

    result.as[PhenotypeWithHpoOutput].collect().head shouldBe PhenotypeWithHpoOutput()
  }


}

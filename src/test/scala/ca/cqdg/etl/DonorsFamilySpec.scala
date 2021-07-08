package ca.cqdg.etl

import bio.ferlab.datalake.spark3.config.DatasetConf
import ca.cqdg.etl.testutils.model._
import ca.cqdg.etl.testutils.{WithSparkSession, testConf}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DonorsFamilySpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  implicit val conf = testConf

  val job = new DonorsFamily()

  val destination        : DatasetConf = job.destination
  val donors             : DatasetConf = job.donors
  val family             : DatasetConf = job.family
  val family_history     : DatasetConf = job.family_history
  val exposure           : DatasetConf = job.exposure

  val inputData = Map(
    donors.id -> Seq(DonorInput()).toDF(),
    family.id -> Seq(FamilyInput(`submitter_donor_id` = "PT00001")).toDF(),
    family_history.id -> Seq(FamilyHistoryInput()).toDF(),
    exposure.id -> Seq(ExposureInput()).toDF()
  )

  it should "transform donors family table" in {

    val result = job.transform(inputData)

    result.as[DonorsFamilyOutput].collect().head shouldBe
      DonorsFamilyOutput(`familyConditions` = List(FAMILYCONDITIONS("No", 20, "Brain cancer", "granddaughter", "FC00001")))
  }

  it should "load donors family table" in {

    val result = job.transform(inputData)
    job.load(result)


    destination.read
      .as[DonorsFamilyOutput].collect().head shouldBe
      DonorsFamilyOutput(`familyConditions` = List(FAMILYCONDITIONS("No", 20, "Brain cancer", "granddaughter", "FC00001")))
  }

}

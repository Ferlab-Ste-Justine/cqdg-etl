package ca.cqdg.etl.testutils

import bio.ferlab.datalake.spark3.config.DatasetConf
import ca.cqdg.etl.testutils.model._
import ca.cqdg.etl.{DonorsFamily, etlConfiguration}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DonorsFamilySpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  val job = new DonorsFamily()(etlConfiguration)

  val destination        : DatasetConf = job.destination
  val donors             : DatasetConf = job.donors
  val family_relationship: DatasetConf = job.family_relationship
  val family_history     : DatasetConf = job.family_history
  val exposure           : DatasetConf = job.exposure

  "run" should "creates donors family table" in {

    val inputData = Map(
      donors -> Seq(DonorInput()).toDF(),
      family_relationship -> Seq(FamilyRelationshipInput(`submitter_donor_id_1` = "PT00001")).toDF(),
      family_history -> Seq(FamilyHistoryInput()).toDF(),
      exposure -> Seq(ExposureInput()).toDF()
    )

    val result = job.transform(inputData)

    result.as[DonorsFamilyOutput].collect().head shouldBe
      DonorsFamilyOutput(`familyConditions` = List(FAMILYCONDITIONS("No",20,"Brain cancer","granddaughter","FC00001")))
  }

}

package ca.cqdg.etl

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import ca.cqdg.etl.testutils.model._
import ca.cqdg.etl.testutils.{WithSparkSession, testConf}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DonorDiagnosisSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  implicit val conf: Configuration = testConf

  val job = new DonorDiagnosis()

  val destination: DatasetConf = job.destination
  val diagnosis: DatasetConf = job.diagnosis
  val treatment: DatasetConf = job.treatment
  val followUp: DatasetConf = job.followUp

  val inputData = Map(
    diagnosis.id -> Seq(DiagnosisInput(`submitter_diagnosis_id` = "DI00453")).toDF(),
    treatment.id -> Seq(TreatmentInput(`submitter_diagnosis_id` = "DI00453")).toDF(),
    followUp.id -> Seq(FollowUpInput(`submitter_diagnosis_id` = "DI00453")).toDF()
  )

  it should "transform donors diagnosis table" in {

    val result = job.transform(inputData)

    result.as[DonorDiagnosisOutput].collect().head shouldBe DonorDiagnosisOutput()
  }

  it should "load donors diagnosis table" in {

    val result = job.transform(inputData)
    job.load(result)
    destination.read.as[DonorDiagnosisOutput].collect().head shouldBe DonorDiagnosisOutput()
  }

}

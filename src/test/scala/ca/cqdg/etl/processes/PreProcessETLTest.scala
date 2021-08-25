package ca.cqdg.etl.processes

import bio.ferlab.datalake.spark3.ClassGenerator
import ca.cqdg.etl.EtlUtils.sanitize
import ca.cqdg.etl.clients.{DictionaryMock, IdServerMock}
import ca.cqdg.etl.models.NamedDataFrame
import ca.cqdg.etl.validation.preprocess.{BiospecimenExpected, DiagnosisExpected, DonorExpected, ExposureExpected, FamilyExpected, FamilyhistoryExpected, FileExpected, FollowupExpected, PhenotypeExpected, SampleregistrationExpected, StudyExpected, TreatmentExpected}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Encoder
import org.scalatest.funsuite.AnyFunSuite

class PreProcessETLTest extends AnyFunSuite with WithSparkSession {

  test("compare expected / transformed files") {

    val input = getClass.getClassLoader.getResource("clinical-data").toString
    val output = createTempFolder("cqdg-etl-pre-process-test")

    val config = new PreProcessETLTestConfig(input, output).preProcessETLConfig
    val etl = new PreProcessETL(new DictionaryMock, new IdServerMock)(spark, config)

    val data = etl.extract()
    assert(data.size == 13)

    val transformed = etl.transform(data)
    assert(transformed.size == 12)

    //etl.load(transformed)

    // following is how to create expected scala class from transformed dataframes
    //generateExpectedFiles(transformed)

    import spark.implicits._

    val biospecimen = getDataFrameResultAs[BiospecimenExpected]("biospecimen", transformed)
    val diagnosis = getDataFrameResultAs[DiagnosisExpected]("diagnosis", transformed)
    val donor = getDataFrameResultAs[DonorExpected]("donor", transformed)
    val exposure = getDataFrameResultAs[ExposureExpected]("exposure", transformed)
    val family = getDataFrameResultAs[FamilyExpected]("family", transformed)
    val familyHistory = getDataFrameResultAs[FamilyhistoryExpected]("family-history", transformed)
    val file = getDataFrameResultAs[FileExpected]("file", transformed)
    val followUp = getDataFrameResultAs[FollowupExpected]("follow-up", transformed)
    val phenotype = getDataFrameResultAs[PhenotypeExpected]("phenotype", transformed)
    val sample = getDataFrameResultAs[SampleregistrationExpected]("sample_registration", transformed)
    val study = getDataFrameResultAs[StudyExpected]("study", transformed)
    val treatment = getDataFrameResultAs[TreatmentExpected]("treatment", transformed)

    println("Assert transformed ...")
    assert(biospecimen.equals(BiospecimenExpected()))
    assert(diagnosis.equals(DiagnosisExpected()))
    assert(donor.equals(DonorExpected()))
    assert(exposure.equals(ExposureExpected()))
    assert(family.equals(FamilyExpected()))
    assert(familyHistory.equals(FamilyhistoryExpected()))
    assert(file.equals(FileExpected()))
    assert(followUp.equals(FollowupExpected()))
    assert(phenotype.equals(PhenotypeExpected()))
    assert(sample.equals(SampleregistrationExpected()))
    assert(study.equals(StudyExpected()))
    assert(treatment.equals(TreatmentExpected()))
  }

  private def getDataFrameResultAs[U: Encoder](name: String, transformed: List[NamedDataFrame]): U = {
    // return first element
    transformed.find(ndf => ndf.name.equals(name)).get.dataFrame.as[U].collect().head
  }

  private def generateExpectedFiles(transformed: List[NamedDataFrame]): Unit = {
    val packageName = "ca.cqdg.etl.validation.preprocess"
    val rootFolder = "src/test/scala/"

    transformed.foreach(ndf => {
      val name = StringUtils.capitalize(sanitize(ndf.name))
      val df = ndf.dataFrame
      // write first element
      ClassGenerator.writeCLassFile(packageName, s"${name}Expected", df, rootFolder)
    })
  }

}

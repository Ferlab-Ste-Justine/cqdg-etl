package ca.cqdg.etl.testutils.model

import bio.ferlab.datalake.spark3.ClassGeneratorImplicits._
import ca.cqdg.etl.test.util.WithSparkSession
import org.apache.spark.sql.DataFrame

object TestClassGenerator extends WithSparkSession with App {


  private def readTSV(path: String): DataFrame = {
    spark.read
      .option("sep", "\t")
      .option("header", "true")
      .csv(getClass.getClassLoader.getResource(path).getFile)
  }

  private def readJSON(path: String): DataFrame = {
    spark.read
      .json(getClass.getClassLoader.getResource(path).getFile)
  }

  val root = "src/test/scala/"

  //readTSV("tsv/input/biospecimen.tsv").writeCLassFile("ca.cqdg.etl.testutils.model", "BiospecimenInput", root)
  //readTSV("tsv/input/diagnosis.tsv").writeCLassFile("ca.cqdg.etl.testutils.model", "DiagnosisInput", root)
  //readTSV("tsv/input/donor.tsv").writeCLassFile("ca.cqdg.etl.testutils.model", "DonorInput", root)
  //readTSV("tsv/input/exposure.tsv").writeCLassFile("ca.cqdg.etl.testutils.model", "ExposureInput", root)
  //readTSV("tsv/input/family-history.tsv").writeCLassFile("ca.cqdg.etl.testutils.model", "FamilyHistoryInput", root)
  //readTSV("tsv/input/family-relationship.tsv").writeCLassFile("ca.cqdg.etl.testutils.model", "FamilyRelationshipInput", root)
  //readTSV("tsv/input/file.tsv")//.writeCLassFile("ca.cqdg.etl.testutils.model", "FileInput", root)
  //readTSV("tsv/input/follow-up.tsv").writeCLassFile("ca.cqdg.etl.testutils.model", "FollowUpInput", root)
  //readTSV("tsv/input/phenotype.tsv").writeCLassFile("ca.cqdg.etl.testutils.model", "PhenotypeInput", root)
  //readTSV("tsv/input/sample_registration.tsv").writeCLassFile("ca.cqdg.etl.testutils.model", "SampleRegistrationInput", root)
  //readTSV("tsv/input/study.tsv").writeCLassFile("ca.cqdg.etl.testutils.model", "StudyInput", root)
  //readTSV("tsv/input/treatment.tsv").writeCLassFile("ca.cqdg.etl.testutils.model", "TreatmentInput", root)

  readJSON("json/output/donors.json").writeCLassFile("ca.cqdg.etl.testutils.model", "DonorOutput", root)
  readJSON("json/output/studies.json").writeCLassFile("ca.cqdg.etl.testutils.model", "StudyOutput", root)
  readJSON("json/output/files.json").writeCLassFile("ca.cqdg.etl.testutils.model", "FileOutput", root)
}

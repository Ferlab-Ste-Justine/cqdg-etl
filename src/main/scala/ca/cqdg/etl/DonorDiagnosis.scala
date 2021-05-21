package ca.cqdg.etl

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import ca.cqdg.etl.utils.EtlUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class DonorDiagnosis()(implicit conf: Configuration) extends ETL()(conf) {

  override val destination: DatasetConf = conf.getDataset("donor_diagnosis")
  val diagnosis: DatasetConf = conf.getDataset("diagnosis")
  val treatment: DatasetConf = conf.getDataset("treatment")
  val followUp: DatasetConf = conf.getDataset("follow_up")


  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      diagnosis.id -> diagnosis.read,
      treatment.id -> treatment.read,
      followUp.id  -> followUp.read
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {

    val diagnosisDf: DataFrame = data(diagnosis.id)
    val treatmentDf: DataFrame = data(treatment.id).drop("diagnosis_ICD_term")
    val followUpDf: DataFrame = data(followUp.id).drop("is_cancer")

    EtlUtils.loadDiagnoses(diagnosisDf, treatmentDf, followUpDf)
  }
}

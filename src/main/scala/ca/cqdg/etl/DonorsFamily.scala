package ca.cqdg.etl

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import ca.cqdg.etl.utils.EtlUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

class DonorsFamily()(implicit conf: Configuration) extends ETL()(conf) {

  override val destination: DatasetConf = conf.getDataset("donor_families")
  val donors              : DatasetConf = conf.getDataset("donors")
  val family_relationship : DatasetConf = conf.getDataset("family_relationship")
  val family_history      : DatasetConf = conf.getDataset("family_history")
  val exposure            : DatasetConf = conf.getDataset("exposure")


  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      donors.id -> donors.read,
      family_relationship.id  -> family_relationship.read,
      family_history.id  -> family_history.read,
      donors.id  -> family_history.read,
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {

    val donorDf: DataFrame = data(donors.id )

    val familyRelationshipDf: DataFrame = data(family_relationship.id )
      .drop("submitter_family_id")

    val familyHistoryDf: DataFrame = data(family_history.id )
      .withColumn("family_condition_age", col("family_condition_age").cast(LongType))

    val exposureDf: DataFrame = data(exposure.id )

    EtlUtils.loadDonors(donorDf, familyRelationshipDf, familyHistoryDf, exposureDf)
  }
}

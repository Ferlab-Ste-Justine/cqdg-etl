package ca.cqdg.etl.utils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataAccessUtils {

  def computeDataAccessByEntityType(dataAccess: DataFrame, entityType: String, entityIdColumnName: String)(implicit spark: SparkSession) = {

    import spark.implicits._

    dataAccess
      .filter(col("entity_type") === entityType)
      .withColumnRenamed("entity_id", entityIdColumnName)
      .withColumn("access_requirements", explode(split($"access_requirements", ";")))
      .filter(EtlUtils.columns.isNotBlank($"access_requirements"))
      .groupBy(entityIdColumnName, "access_limitations")
      .agg(
        collect_set(
          trim($"access_requirements").as("access_requirements")
        ).as("access_requirements")
      ).groupBy(entityIdColumnName)
      .agg(
        first(
          struct(cols =
            $"access_limitations",
            $"access_requirements"
          )
        ).as("data_access_codes")
      ).as("dataAccessGroup")
  }
}

package ca.cqdg.etl.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, explode, split, struct, trim}

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
        collect_list(
          struct(cols =
            trim($"access_requirements").as("key")
          )
        ).as("access_requirements")
      ).groupBy(entityIdColumnName)
      .agg(
        collect_list(
          struct(cols =
            $"access_limitations",
            $"access_requirements"
          )
        ).as("data_access")
      ).as("dataAccessGroup")
  }
}

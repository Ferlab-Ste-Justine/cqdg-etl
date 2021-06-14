package ca.cqdg.etl.utils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataAccessUtils {

  def computeDataAccessByEntityType(dataAccess: DataFrame,
                                    entityType: String,
                                    entityIdColumnName: String,
                                    duoCodeList: DataFrame)(implicit spark: SparkSession) = {

    import spark.implicits._

    dataAccess
      .filter(col("entity_type") === entityType)
      .withColumnRenamed("entity_id", entityIdColumnName)
      .withColumn("access_requirements", explode(split($"access_requirements", ";")))
      .filter(EtlUtils.columns.isNotBlank($"access_requirements"))
      .withColumn("access_requirements", trim($"access_requirements"))
      .join(duoCodeList, $"access_limitations" === $"id", "left")
      .drop("access_limitations")
      .select(
        $"entity_type",
        col(entityIdColumnName),
        concat($"name", lit(" ("), $"id", lit(")")) as "access_limitations",
        $"access_requirements")
      .join(duoCodeList, $"access_requirements" === $"id", "left")
      .drop("access_requirements")
      .select(
        $"entity_type",
        col(entityIdColumnName),
        $"access_limitations",
        concat($"name", lit(" ("), $"id", lit(")")) as "access_requirements")
      .groupBy(entityIdColumnName, "access_limitations")
      .agg(
        collect_set($"access_requirements").as("access_requirements")
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

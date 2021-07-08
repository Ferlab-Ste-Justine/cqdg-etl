package ca.cqdg.etl.utils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DataAccessUtils {

  def computeDataAccessByEntityType(study: DataFrame,
                                    duoCodeList: DataFrame)(implicit spark: SparkSession): Dataset[Row] = {

    import spark.implicits._

    study
      .select("study_id","access_limitations", "access_requirements")
      .withColumn("access_requirements", explode(split($"access_requirements", ";")))
      .filter(EtlUtils.columns.isNotBlank($"access_requirements"))
      .withColumn("access_requirements", trim($"access_requirements"))
      .join(duoCodeList, $"access_limitations" === $"id", "left")
      .drop("access_limitations")
      .select(
        col("study_id"),
        concat($"name", lit(" ("), $"id", lit(")")) as "access_limitations",
        $"access_requirements")
      .join(duoCodeList, $"access_requirements" === $"id", "left")
      .drop("access_requirements")
      .select(
        col("study_id"),
        $"access_limitations",
        concat($"name", lit(" ("), $"id", lit(")")) as "access_requirements")
      .groupBy("study_id", "access_limitations")
      .agg(
        collect_set($"access_requirements").as("access_requirements")
      ).groupBy("study_id")
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

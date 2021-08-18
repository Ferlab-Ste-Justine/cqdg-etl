package ca.cqdg.etl.model

import org.apache.spark.sql.DataFrame

case class NamedDataFrame(
                           name: String,
                           dataFrame: DataFrame,
                           var studyVersion: String,
                           var studyVersionCreationDate: String,
                           var dictionaryVersion: String
                         )

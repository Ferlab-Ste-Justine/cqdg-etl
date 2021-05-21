package ca.cqdg.etl.model

import org.apache.spark.sql.DataFrame

case class NamedDataFrame(
                           name: String,
                           dataFrame: DataFrame,
                           studyVersion: String,
                           studyVersionCreationDate: String,
                           dictionaryVersion: String
                         )

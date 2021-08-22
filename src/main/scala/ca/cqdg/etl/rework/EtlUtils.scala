package ca.cqdg.etl.rework

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{lit, trim, udf, when}
import org.apache.spark.sql.types.StringType

import java.text.{Normalizer, SimpleDateFormat}
import java.time.{LocalDate, Period, ZoneId}
import scala.util.{Random, Try}

object EtlUtils {

  val dateFormats = Seq(
    new SimpleDateFormat("d/M/yyyy"),
    new SimpleDateFormat("d/MM/yyyy"),
    new SimpleDateFormat("dd/M/yyyy"),
    new SimpleDateFormat("dd/MM/yyyy")
  )

  def sanitize(str: String): String = {
    val noExtension = if (str.indexOf(".") > -1) str.substring(0, str.indexOf(".")) else str;
    val normalized = Normalizer.normalize(noExtension, Normalizer.Form.NFD)
    val noSpecialChars = normalized.replaceAll("[^a-zA-Z]", "")
    noSpecialChars.toLowerCase().trim()
  }

  def parseDate(date: String): Option[LocalDate] = {
    dateFormats.toStream.map(formatter => {
      Try(formatter.parse(date).toInstant.atZone(ZoneId.systemDefault()).toLocalDate).toOption
    }).find(_.nonEmpty).flatten
  }

}

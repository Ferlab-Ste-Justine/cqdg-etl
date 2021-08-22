package ca.cqdg.etl.rework

import java.text.{Normalizer, SimpleDateFormat}
import java.time.{LocalDate, ZoneId}
import scala.util.Try

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

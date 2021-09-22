package ca.cqdg.etl

import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate

class EtlUtilsTest extends AnyFunSuite {
  
  test("parseDate") {
    assert(EtlUtils.parseDate("foo").isEmpty)
    assert(EtlUtils.parseDate(null).isEmpty)
    val validDate = LocalDate.of(2021,9,2);
    assert(EtlUtils.parseDate("2/9/2021").get.equals(validDate))
    assert(EtlUtils.parseDate("2/09/2021").get.equals(validDate))
    assert(EtlUtils.parseDate("02/9/2021").get.equals(validDate))
    assert(EtlUtils.parseDate("02/09/2021").get.equals(validDate))
  }

}

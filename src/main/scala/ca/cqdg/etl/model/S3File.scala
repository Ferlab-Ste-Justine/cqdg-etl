package ca.cqdg.etl.model

import ca.cqdg.etl.utils.EtlUtils.sanitize

case class S3File(bucket: String, key: String, md5: String, size: Long) {
  val parentKey: String = key.substring(0, key.lastIndexOf("/"))
  val filename: String = key.substring(key.lastIndexOf("/") + 1)
  val schema: String = sanitize(filename)
}

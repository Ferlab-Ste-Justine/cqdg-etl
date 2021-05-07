package ca.cqdg.etl.utils

import ca.cqdg.etl.model.S3File
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectListing

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object S3Utils {

  def ls(bucket: String, prefix: String, s3Client: AmazonS3): List[S3File] =
    nextBatch(s3Client, s3Client.listObjects(bucket, prefix))

  @tailrec
  private def nextBatch(s3Client: AmazonS3, listing: ObjectListing, objects: List[S3File] = Nil): List[S3File] = {
    val pageKeys = listing.getObjectSummaries.asScala.map(o => S3File(o.getBucketName, o.getKey, o.getETag, o.getSize)).toList

    if (listing.isTruncated) {
      nextBatch(s3Client, s3Client.listNextBatchOfObjects(listing), pageKeys ::: objects)
    } else
      pageKeys ::: objects
  }

  def loadFileEntries(bucket: String, prefix: String, s3Client: AmazonS3): Map[String, List[S3File]] = {
    val fileEntries = ls(bucket, prefix, s3Client)
    // Group by "folder" and remove all "folders" not containing a study_version_metadata.json file.
    val filesGroupedByKey = fileEntries
      .groupBy(f => f.parentKey)
      .filter(entry =>
        entry._2.exists(f => f.filename == "study_version_metadata.json") &&
        !entry._2.exists(f => f.filename == "_SUCCESS"))  // Indicator that it has already been processed
    filesGroupedByKey
  }

  def writeSuccessIndicator(bucket: String, prefix: String, s3Client: AmazonS3): Unit = {
    val key: String = if (prefix.endsWith("/")) s"${prefix}_SUCCESS" else s"${prefix}/_SUCCESS"
    s3Client.putObject(bucket, key, "");
  }
}

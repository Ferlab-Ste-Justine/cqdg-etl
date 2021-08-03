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

  def loadFileEntries(bucket: String, prefix: String, s3Client: AmazonS3, hasMetadata: Boolean = true): Map[String, List[S3File]] = {
    val fileEntries = ls(bucket, prefix, s3Client)
    // Group by "folder" and remove all "folders" not containing a study_version_metadata.json file.
    val filesGroupedByKey = fileEntries
      .groupBy(f => f.parentKey)
      .filter(entry =>
        (!hasMetadata || entry._2.exists(f => f.filename == "study_version_metadata.json")) &&
        !entry._2.exists(f => f.filename == "_SUCCESS"))  // Indicator that it has already been processed
    filesGroupedByKey
  }

  def loadPreProcessedEntries(bucket: String, prefix: String, s3Client: AmazonS3): Map[String, List[String]] = {

    val filesGroupedByKey = ls(bucket, prefix, s3Client) // list all S3 files
      .groupBy(f => f.parentKey)
      .filter(entry => entry._2.exists(f => f.filename == "_SUCCESS")) // only if successfully saved by spark


    val filesGroupedBySameFolder = filesGroupedByKey.map({case(path, _) =>
      val parent =  path.substring(0, path.lastIndexOf("/"))  // extract parent name
      val files = filesGroupedByKey
        .filter(entry => entry._1.contains(parent)) // if same parent then group in same set
        .values.flatten   // List[List[S3File] => List[S3File]
        .map(f => f.parentKey)
        .toList.distinct  // avoid duplicates
      (parent, files)
    })

    // filter folders that have already been processed (contains a _SUCCESS file)
    val filterOnlyNotProcessed = filesGroupedBySameFolder.filter({case (path, _) =>
      val alreadyProcessed = s3Client.doesObjectExist(bucket, s"$path/_SUCCESS")
      !alreadyProcessed
    })

    filterOnlyNotProcessed
  }

  def loadFileEntry(bucket: String, prefix: String, s3Client: AmazonS3) = {
    s3Client.listObjects(bucket, prefix)
      .getObjectSummaries.asScala
      .map(o => S3File(o.getBucketName, o.getKey, o.getETag, o.getSize))
      .toList
  }

  def writeSuccessIndicator(bucket: String, prefix: String, s3Client: AmazonS3): Unit = {
    val key: String = if (prefix.endsWith("/")) s"${prefix}_SUCCESS" else s"${prefix}/_SUCCESS"
    s3Client.putObject(bucket, key, "");
  }
}

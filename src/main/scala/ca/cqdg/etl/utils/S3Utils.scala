package ca.cqdg.etl.utils

import ca.cqdg.etl.EtlApp.s3Endpoint
import ca.cqdg.etl.model.S3File
import ca.cqdg.etl.utils.EtlUtils.getConfiguration
import com.amazonaws.ClientConfiguration
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object S3Utils {

  val clientConfiguration = new ClientConfiguration
  clientConfiguration.setSignerOverride("AWSS3V4SignerType")

  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withEndpointConfiguration(
      new EndpointConfiguration(
        getConfiguration("SERVICE_ENDPOINT", s3Endpoint),
        getConfiguration("AWS_DEFAULT_REGION", Regions.US_EAST_1.name()))
    )
    .withPathStyleAccessEnabled(true)
    .withClientConfiguration(clientConfiguration)
    .build()

  def ls(bucket: String, prefix: String): List[S3File] = nextBatch(s3Client, s3Client.listObjects(bucket, prefix))

  @tailrec
  private def nextBatch(s3Client: AmazonS3, listing: ObjectListing, objects: List[S3File] = Nil): List[S3File] = {
    val pageKeys = listing.getObjectSummaries.asScala.map(o => S3File(o.getBucketName, o.getKey, o.getETag, o.getSize)).toList

    if (listing.isTruncated) {
      nextBatch(s3Client, s3Client.listNextBatchOfObjects(listing), pageKeys ::: objects)
    } else
      pageKeys ::: objects
  }

  def loadFileEntries(bucket: String, prefix: String): Map[String, List[S3File]] = {
    val fileEntries = ls(bucket, prefix)
    // Group by "folder" and remove all "folders" not containing a study_version_metadata.json file.
    val filesGroupedByKey = fileEntries
      .groupBy(f => f.parentKey)
      .filter(entry =>
        entry._2.exists(f => f.filename == "study_version_metadata.json") &&
        !entry._2.exists(f => f.filename == "_SUCCESS"))  // Indicator that it has already been processed
    filesGroupedByKey
  }

  def writeSuccessIndicator(bucket: String, prefix: String): Unit = {
    val key: String = if (prefix.endsWith("/")) s"${prefix}_SUCCESS" else s"${prefix}/_SUCCESS"
    s3Client.putObject(bucket, key, "");
  }
}

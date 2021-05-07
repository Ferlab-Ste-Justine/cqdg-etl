package ca.cqdg.etl

import ca.cqdg.etl.EtlApp.filesPerFolder
import ca.cqdg.etl.model.{NamedDataFrame, S3File}
import ca.cqdg.etl.test.util.WithSparkSession
import ca.cqdg.etl.utils.PreProcessingUtils.preProcess
import ca.cqdg.etl.utils.{PreProcessingUtils, S3Utils}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class DonorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll with WithSparkSession
//  with DockerTestKit with DockerKitSpotify
//  with MinioS3MockService
{

//  implicit val pc: PatienceConfig = PatienceConfig(Span(20, Seconds), Span(1, Second))
  val BUCKET_NAME = "cqdg"

  //  override def beforeAll(): Unit = {
  val s3Credential = new BasicAWSCredentials("minioadmin", "minioadmin")
  val s3Client: AmazonS3 = AmazonS3ClientBuilder
    .standard()
    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:9000", Regions.US_EAST_1.name()))
    .withPathStyleAccessEnabled(true)
    .withCredentials(new AWSStaticCredentialsProvider(s3Credential))
    .build()

  val inputDirectory = new File("../cqdg-etl/src/test/resources/tsv/input")

  if (!s3Client.doesBucketExistV2(BUCKET_NAME)) {
    s3Client.createBucket(BUCKET_NAME)

    if (inputDirectory.exists && inputDirectory.isDirectory) {
      val list =  inputDirectory.listFiles.filter(_.isFile).map(_.getName).toList
      list.foreach(fn => {
        val file = new File(s"../cqdg-etl/src/test/resources/tsv/input/$fn")
        s3Client.putObject(
          BUCKET_NAME,
          s"clinical-data/$fn",
          file
        )
      })
    }
  }
  //  val toto = s3Client.listObjects(BUCKET_NAME, "clinical-data").getObjectSummaries

  val filesPerFolder: Map[String, List[S3File]] = S3Utils.loadFileEntries(BUCKET_NAME, "clinical-data", s3Client)
  val readyToProcess: Map[String, List[NamedDataFrame]] = PreProcessingUtils.preProcess(filesPerFolder, BUCKET_NAME)
//  val readyToProcess = PreProcessingUtils.toto()
//  println(readyToProcess)

//  }
  "Donors" should "map hpo terms per donors" in {
//    startAllOrFail(minioContainer).futureValue shouldBe true
    1 shouldBe 1
  }
}

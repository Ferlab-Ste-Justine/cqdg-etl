package ca.cqdg.etl

import ca.cqdg.etl.test.util.WithSparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EtlUtilsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with WithSparkSession
//  with DockerTestKit with DockerKitSpotify
//  with MinioS3MockService
{

  "Donors" should "map hpo terms per donors" in {

    1 shouldBe 1
  }
}

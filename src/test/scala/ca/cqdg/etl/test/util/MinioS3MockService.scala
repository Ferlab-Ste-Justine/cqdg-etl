package ca.cqdg.etl.test.util

import com.whisk.docker.{DockerContainer, DockerKit, DockerReadyChecker}


/**
 * docker run -p 9000:9000 minio/minio server /data
 */
trait MinioS3MockService extends DockerKit {

  val MinioPort = 9000

  val minioContainer: DockerContainer = DockerContainer("minio/minio")
    //      .withVolumes(Seq(VolumeMapping("/mnt/data", "/data"), VolumeMapping("/mnt/config", "/root/.minio")))
    .withPorts(MinioPort -> Some(MinioPort))
//    .withReadyChecker(DockerReadyChecker.LogLineContains("Drive Capacity:"))
    .withCommand("server", "/data")

  abstract override def dockerContainers: List[DockerContainer] =
    minioContainer :: super.dockerContainers
}

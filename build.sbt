name := "cqdg-etl"

version := "0.1"

scalaVersion := "2.12.12"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val spark_version = "3.0.0"

resolvers += "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots"
resolvers += "Sonatype OSS Releases" at "https://s01.oss.sonatype.org/content/repositories/releases"

val dockerTestkitVersion = "0.9.6"

/* Runtime */
libraryDependencies += "org.apache.spark" %% "spark-core" % spark_version % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % spark_version % Provided
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.0" % Provided
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.0" % Provided
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.0" % Provided
libraryDependencies += "io.projectglow" %% "glow-spark3" % "1.0.0" exclude ("org.apache.hadoop", "hadoop-client")
libraryDependencies += "io.delta" %% "delta-core" % "0.8.0" % Provided
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bom" % "1.11.975"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.975"
libraryDependencies += "com.softwaremill.sttp.client3" %% "core" % "3.1.7"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.6"
libraryDependencies += "bio.ferlab" %% "datalake-spark3" % "0.0.41"
libraryDependencies += "com.typesafe" % "config" % "1.4.1"
libraryDependencies += "org.keycloak" % "keycloak-authz-client" % "12.0.3"

/* Test */
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.6" % "test"
libraryDependencies += "org.scalamock" %% "scalamock" % "5.1.0" % "test"
libraryDependencies += "org.apache.spark" %% "spark-hive" % spark_version % "test"
libraryDependencies += "org.scalatestplus" %% "mockito-3-4" % "3.2.8.0" % "test"

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => MergeStrategy.last
  case "META-INF/DISCLAIMER" => MergeStrategy.last
  case "mozilla/public-suffix-list.txt" => MergeStrategy.last
  case "overview.html" => MergeStrategy.last
  case "git.properties" => MergeStrategy.discard
  case "mime.types" => MergeStrategy.first
  case PathList("scala", "annotation", "nowarn.class" | "nowarn$.class") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
assemblyJarName in assembly := "cqdg-etl.jar"
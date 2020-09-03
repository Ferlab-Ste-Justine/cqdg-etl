name := "cqdg-etl"

version := "0.1"

scalaVersion := "2.12.12"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val spark_version = "3.0.0"

/* Runtime */
libraryDependencies +=  "org.apache.spark" %% "spark-core" % spark_version % Provided
libraryDependencies +=  "org.apache.spark" %% "spark-sql" % spark_version % Provided
libraryDependencies +=  "org.apache.hadoop" % "hadoop-client" % "3.2.0" % Provided
libraryDependencies +=  "org.apache.hadoop" % "hadoop-aws" % "3.2.0" % Provided
libraryDependencies += "io.projectglow" %% "glow" % "0.5.0" exclude ("org.apache.hadoop", "hadoop-client")
libraryDependencies += "io.delta" %% "delta-core" % "0.7.0" % Provided

/* Test */
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % "test"
libraryDependencies += "org.apache.spark" %% "spark-hive" % spark_version % "test"

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => MergeStrategy.last
  case "META-INF/DISCLAIMER" => MergeStrategy.last
  case "mozilla/public-suffix-list.txt" => MergeStrategy.last
  case "overview.html" => MergeStrategy.last
  case "git.properties" => MergeStrategy.discard
  case "mime.types" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
assemblyJarName in assembly := "cqdg-etl.jar"
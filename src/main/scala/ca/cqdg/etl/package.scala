package ca.cqdg

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.loader.Format.{CSV, JSON, PARQUET}
import bio.ferlab.datalake.spark3.loader.LoadType.OverWrite
import ca.cqdg.etl.utils.EtlUtils.getConfiguration
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

package object etl {

  val clinical_data: String = "clinical-data"
  val ontology_input: String = "ontology-input"
  val clinical_data_etl_indexer: String = "clinical-data-etl-indexer"
  val tsv_with_headers = Map("sep" -> "\t", "header" -> "true")
  val tsv_without_headers = Map("sep" -> "\t", "header" -> "false")

  implicit val executorContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool()) // better than scala global executor + keep the App alive until shutdown
  // properly shutdown after app execution
  sys.addShutdownHook(executorContext.shutdown())

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("CQDG-ETL")
    //.master("local")
    .getOrCreate()

  val s3Config: Config = ConfigFactory.load.getObject("s3").toConfig
  val s3Endpoint: String = s3Config.getString("endpoint")
  val s3Bucket: String = s3Config.getString("bucket")
  val s3Region: String = s3Config.getString("region")
  val s3ClientId: String =s3Config.getString("client-id")
  val s3SecretKey: String =s3Config.getString("secret-key")

  spark.sparkContext.setLogLevel("WARN")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3Endpoint)
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3ClientId)
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3SecretKey)
  sys.addShutdownHook(spark.stop())

  val clientConfiguration = new ClientConfiguration
  clientConfiguration.setSignerOverride("AWSS3V4SignerType")

  val s3Client: AmazonS3 = AmazonS3ClientBuilder
    .standard()
    .withEndpointConfiguration(new EndpointConfiguration(s3Endpoint, s3Region))
    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(s3ClientId, s3SecretKey)))
    .withPathStyleAccessEnabled(true)
    .withClientConfiguration(clientConfiguration)
    .build()

  val etlConfiguration: Configuration = Configuration(
    storages = List(
      StorageConf(clinical_data, s"s3a://cqdg/${clinical_data}"),
      StorageConf(ontology_input, s"s3a://cqdg/${ontology_input}"),
      StorageConf(clinical_data_etl_indexer, s"s3a://cqdg/${clinical_data_etl_indexer}")),
    sources = List(
      //raw data
      DatasetConf("study_version_metadata", clinical_data, "/*/*/*/study_version_metadata.json", JSON, OverWrite),
      DatasetConf("biospecimen"           , clinical_data, "/*/*/*/biospecimen.tsv"            , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("diagnosis"             , clinical_data, "/*/*/*/diagnosis.tsv"              , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("donor"                 , clinical_data, "/*/*/*/donor.tsv"                  , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("exposure"              , clinical_data, "/*/*/*/exposure.tsv"               , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("family_history"        , clinical_data, "/*/*/*/family-history.tsv"         , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("family"                , clinical_data, "/*/*/*/family.tsv"                 , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("file"                  , clinical_data, "/*/*/*/file.tsv"                   , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("follow_up"             , clinical_data, "/*/*/*/follow-up.tsv"              , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("phenotype"             , clinical_data, "/*/*/*/phenotype.tsv"              , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("sample_registration"   , clinical_data, "/*/*/*/sample_registration.tsv"    , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("study"                 , clinical_data, "/*/*/*/study.tsv"                  , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("treatment"             , clinical_data, "/*/*/*/treatment.tsv"              , CSV , OverWrite, readoptions = tsv_with_headers),

      DatasetConf("hpo"                   , ontology_input, "/hpo_terms.json.gz"               , JSON, OverWrite),
      DatasetConf("mondo"                 , ontology_input, "/mondo_terms.json.gz"             , JSON, OverWrite),

      //intermediate data
      DatasetConf("donor_diagnosis", clinical_data_etl_indexer, "/donor_diagnosis", PARQUET, OverWrite),
      DatasetConf("donor_families" , clinical_data_etl_indexer, "/donor_families" , PARQUET, OverWrite),


      //data to index
      DatasetConf("donors" , clinical_data_etl_indexer, "/donors" ,  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date")),
      DatasetConf("studies", clinical_data_etl_indexer, "/studies",  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date")),
      DatasetConf("files"  , clinical_data_etl_indexer, "/files"  ,  JSON, OverWrite, partitionby = List("study_id", "dictionary_version", "study_version", "study_version_creation_date"))
    )
  )
}

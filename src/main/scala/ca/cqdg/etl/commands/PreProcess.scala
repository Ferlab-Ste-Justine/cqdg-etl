package ca.cqdg.etl.commands

import ca.cqdg.etl.clients.{DictionaryClient, IdServerClient}
import ca.cqdg.etl.processes.{PreProcessETL, PreProcessETLConfig}
import picocli.CommandLine.{Command, Option}

@Command(name = "pre-process", mixinStandardHelpOptions = true, description = Array("pre-process the input TSV files"), version = Array("0.1"))
class PreProcess() extends Runnable {

  @Option(names = Array("-i", "--input"), required = true, description = Array("input TSV files location, ex: s3a://cqdg/clinical-data/e2adb961-4f58-4e13-a24f-6725df802e2c/11-PLA-STUDY/15"))
  var input: String = null

  @Option(names = Array("-o", "--output"), required = true, description = Array("where the transformed files will be saved, ex: s3a://cqdg/clinical-data-with-ids/e2adb961-4f58-4e13-a24f-6725df802e2c/11-PLA-STUDY/15"))
  var output: String = null

  @Option(names = Array("-d", "--dev"), description = Array(""))
  var isDev: Boolean = false

  override def run(): Unit = {
    val config = new PreProcessETLConfig(input, output)
    val dictionaryClient = new DictionaryClient
    val idServerClient = new IdServerClient
    val etl = new PreProcessETL(dictionaryClient, idServerClient)(SparkConfig.getSparkSession(isDev), config.preProcessETLConfig)
    etl.load(etl.transform(etl.extract()))
  }

}

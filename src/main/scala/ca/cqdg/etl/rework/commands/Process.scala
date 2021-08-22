package ca.cqdg.etl.rework.commands

import ca.cqdg.etl.rework.clients.{DictionaryClient, IdServerClient}
import ca.cqdg.etl.rework.processes.{PreProcessETL, PreProcessETLConfig, ProcessETL, ProcessETLConfig}
import picocli.CommandLine.{Command, Option}

@Command(name = "process", mixinStandardHelpOptions = true, description = Array("process pre-processed data"), version = Array("0.1"))
class Process() extends Runnable {

  @Option(names = Array("-i", "--input"), required = true, description = Array("input pre-processed data location, ex: s3a://cqdg/clinical-data-with-ids/e2adb961-4f58-4e13-a24f-6725df802e2c/11-PLA-STUDY/15"))
  var input: String = null

  @Option(names = Array("-t", "--ontology"), required = true, description = Array("ontology files location, ex: s3a://cqdg/ontology-input"))
  var ontology: String = null

  @Option(names = Array("-o", "--output"), required = true, description = Array("where the transformed files will be saved, ex: s3a://cqdg/clinical-data-etl-indexer"))
  var output: String = null

  @Option(names = Array("-l", "--local"), description = Array(""))
  var isLocal: Boolean = false

  override def run(): Unit = {
    val config = new ProcessETLConfig(input, ontology, output)
    val etl = new ProcessETL()(SparkConfig.getSparkSession(isLocal), config.processETLConfig)
    etl.transform(etl.extract())
  }

}

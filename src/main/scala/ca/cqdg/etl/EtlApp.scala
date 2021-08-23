package ca.cqdg.etl

import ca.cqdg.etl.commands.{PreProcess, Process}
import picocli.CommandLine
import picocli.CommandLine.Command

@Command(name = "cqdg-etl", mixinStandardHelpOptions = true,
  version = Array("0.1"),
  description = Array("CQDG Extract-transform-load"),
  subcommands = Array(classOf[PreProcess], classOf[Process]))
class EtlApp extends Runnable {
  override def run(): Unit = {
    // nothing to do
  }
}

object EtlApp {
  val commandLine = new CommandLine(new EtlApp)

  def main(args: Array[String]): Unit = {
    if (args.nonEmpty) {
      System.exit(commandLine.execute(args: _*))
    } else { // display usage if no sub-command
      commandLine.usage(System.out)
    }
  }
}
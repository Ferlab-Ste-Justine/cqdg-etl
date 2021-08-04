package ca.cqdg.etl

object EtlApp extends App {

  PreProcess.run()

  Process.run()

}

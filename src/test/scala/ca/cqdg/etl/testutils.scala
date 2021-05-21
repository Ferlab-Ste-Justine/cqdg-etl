package ca.cqdg.etl

import bio.ferlab.datalake.spark3.config.{Configuration, StorageConf}

package object testutils {

  val testStorages = List(
    StorageConf(clinical_data, this.getClass.getResource(".").getFile + "clinical-data"),
    StorageConf(clinical_data_etl_indexer, this.getClass.getResource(".").getFile + "clinical-data-etl-indexer")
  )

  val testConf: Configuration = etlConfiguration.copy(storages = testStorages)

}

package ca.cqdg.etl.testutils.model

case class DataAccessInput(`entity_type`: String = "study",
                         `entity_id`: String = "ST0003",
                         `access_limitations`: String = "DUO:0000005",
                         `access_requirements`: String = "DUO:0000021; DUO:0000025;  DUO:0000026; DUO:0000027; DUO:0000029;")
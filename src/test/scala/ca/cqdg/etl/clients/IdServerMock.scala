package ca.cqdg.etl.clients

import ca.cqdg.etl.clients.inf.IIdServer

import scala.io.Source

class IdServerMock extends IIdServer{

    val hash = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("idserver/hash.json")).getLines().mkString

    override def getCQDGIds(payload: String): String = hash

}

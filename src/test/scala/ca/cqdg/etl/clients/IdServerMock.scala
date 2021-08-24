package ca.cqdg.etl.clients

import ca.cqdg.etl.clients.inf.IIdServer
import com.google.gson.JsonParser

class IdServerMock extends IIdServer{

    override def getCQDGIds(payload: String): String = {
      val response = new StringBuilder()
      val json = new JsonParser().parse(payload).getAsJsonObject.entrySet()
      assert(json.size() > 0)
      json.forEach(el => {
        response.append(String.format(s"{'hash':'${el.getKey}','internal_id':'internal_id'},"))
      })
      s"[${response.deleteCharAt(response.size-1).toString}]"
    }

}

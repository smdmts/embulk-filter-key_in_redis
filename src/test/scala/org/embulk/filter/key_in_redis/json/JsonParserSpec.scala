package org.embulk.filter.key_in_redis.json

import org.scalatest.{FlatSpec, Matchers}

class JsonParserSpec extends FlatSpec with Matchers {

  it should "be parse" in {
    val sequence1 = JsonParser("""
        |{
        |  "name" : "John",
        |  "age" : 50
        |}
      """.stripMargin)

    val sequence2 = JsonParser("{\"a\":1 , \"c\":2 }")
  }
}

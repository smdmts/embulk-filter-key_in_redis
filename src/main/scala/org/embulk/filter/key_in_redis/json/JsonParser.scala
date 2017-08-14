package org.embulk.filter.key_in_redis.json

import io.circe._
import io.circe.parser._

object JsonParser {
  def apply(json: String): Map[String, String] = {
    decode[Map[String, Json]](json) match {
      case Right(v: Map[String, Json]) =>
        v.map {
          case (key, innerValue) =>
            val value = innerValue.asString
              .orElse(innerValue.asNumber.map(_.toString))
              .orElse(innerValue.asBoolean.map(_.toString))
              .getOrElse("") // empty.
            if (innerValue.isArray | innerValue.isObject) {
              sys.error(s"not supported json type. key=$key value=$innerValue")
            }
            (key, value)
        }
      case _ =>
        sys.error(s"could not parse json. $json")
    }
  }
}

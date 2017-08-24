package org.embulk.filter.key_in_redis.row

import java.security.MessageDigest

import org.bouncycastle.util.encoders.Hex
import org.embulk.filter.key_in_redis.json.JsonParser
import org.embulk.spi.time.TimestampFormatter
import org.embulk.spi.{
  Column,
  PageBuilder,
  PageReader,
  ColumnVisitor => EmbulkColumnVisitor
}

case class SetValueColumnVisitor(reader: PageReader,
                                 timestampFormatter: TimestampFormatter,
                                 keyMap: Map[String, String],
                                 jsonKeyMap: Map[String, String],
                                 appender: String,
                                 matchAsMd5: Boolean)
    extends EmbulkColumnVisitor {
  import scala.collection.mutable
  private val recordMap = mutable.Map[String, String]()
  private val valueHolderSet = mutable.Set[ValueHolder[_]]()

  val digestMd5: MessageDigest = MessageDigest.getInstance("MD5")
  val parameterKeys: Seq[String] = keyMap.values.toSeq
  val jsonKeys: Seq[String] = jsonKeyMap.values.toSeq
  val sortedKeys: List[String] = {
    val overRapped =
      (keyMap.keys ++ jsonKeyMap.values.toSeq)
        .groupBy(identity)
        .mapValues(_.size)
        .values
        .toSet
    if (overRapped.size > 1) {
      sys.error("same index number is defined.")
    }
    (keyMap ++ jsonKeyMap).toList.sortBy(_._1.toInt).map(_._2)
  }

  override def timestampColumn(column: Column): Unit =
    value(column, reader.getTimestamp).foreach(v =>
      put(column, timestampFormatter.format(v)))

  override def stringColumn(column: Column): Unit =
    value(column, reader.getString).foreach { v =>
      put(column, v)
    }

  override def longColumn(column: Column): Unit =
    value(column, reader.getLong).foreach(v => put(column, v.toString))

  override def doubleColumn(column: Column): Unit =
    value(column, reader.getDouble).foreach(v => put(column, v.toString))

  override def booleanColumn(column: Column): Unit =
    value(column, reader.getBoolean).foreach(v => put(column, v.toString))

  override def jsonColumn(column: Column): Unit =
    value(column, reader.getJson).foreach { v =>
      if (jsonKeys.nonEmpty) {
        val map = JsonParser(v.toJson)
        jsonKeys.foreach { key =>
          map.get(key) match {
            case Some(value) =>
              recordMap.put(key, value)
            case None =>
          }
        }
      }
      put(column, v.toJson)
    }

  def value[A](column: Column, method: => (Column => A)): Option[A] = {
    val result = if (reader.isNull(column)) {
      None
    } else {
      Some(method(column))
    }
    valueHolderSet.add(ValueHolder(column, result))
    result
  }

  def put(column: Column, value: String): Unit = {
    if (parameterKeys.contains(column.getName)) {
      recordMap.put(column.getName, value)
    }
    ()
  }

  lazy val matchKey: String = {
    val keys = sortedKeys
      .flatMap { key =>
        recordMap.get(key)
      }
      .mkString(appender)
    if (matchAsMd5) {
      Hex.toHexString(digestMd5.digest(keys.getBytes()))
    } else keys
  }

  def getRow(pageBuilder: PageBuilder): Row =
    Row(matchKey, valueHolderSet, pageBuilder)

}

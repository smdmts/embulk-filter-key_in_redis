package org.embulk.filter.key_in_redis

import java.security.MessageDigest

import com.google.common.base.Optional
import org.bouncycastle.util.encoders.Hex
import org.embulk.filter.key_in_redis.column._

import scala.collection.mutable.ListBuffer

import scala.collection.JavaConverters._
import org.embulk.spi.time.TimestampFormatter
import org.embulk.spi.{
  Exec,
  Page,
  PageBuilder,
  PageReader,
  Schema,
  PageOutput => EmbulkPageOutput
}

case class PageOutput(task: PluginTask,
                      schema: Schema,
                      output: EmbulkPageOutput)
    extends EmbulkPageOutput {
  val pageBuilder = new PageBuilder(Exec.getBufferAllocator, schema, output)
  def timestampFormatter(): TimestampFormatter =
    new TimestampFormatter(task, Optional.absent())

  val digestMd5: MessageDigest = MessageDigest.getInstance("MD5")

  override def add(page: Page): Unit = {
    val reader: PageReader = new PageReader(schema)
    reader.setPage(page)
    val handlerBuffer = new ListBuffer[PageHandler]()
    while (reader.nextRecord()) {
      val setValueVisitor = SetValueColumnVisitor(
        reader,
        timestampFormatter(),
        task.getKeyWithIndex.asScala.toMap,
        task.getJsonKeyWithIndex.asScala.toMap,
        task.getAppender)
      schema.visitColumns(setValueVisitor)
      val matchValue = if (task.getMatchAsMD5) {
        Hex.toHexString(digestMd5.digest(setValueVisitor.getValue.getBytes()))
      } else setValueVisitor.getValue
      handlerBuffer.append(
        PageHandler(matchValue, PassthroughColumnVisitor(reader, pageBuilder)))
    }
    reader.close()
    KeyInRedisFilterPlugin.redis.foreach { redis =>
      val result = redis.exists(handlerBuffer.map(_.matchValue))
      handlerBuffer.foreach { value =>
        if (!result(value.matchValue)) {
          schema.visitColumns(value.visitor)
          value.visitor.addRecord()
        }
      }
    }

  }

  override def finish(): Unit = pageBuilder.finish()
  override def close(): Unit = pageBuilder.close()

}

case class PageHandler(matchValue: String, visitor: PassthroughColumnVisitor)

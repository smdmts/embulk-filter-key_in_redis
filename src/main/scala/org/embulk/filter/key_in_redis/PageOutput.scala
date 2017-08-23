package org.embulk.filter.key_in_redis

import java.security.MessageDigest

import com.google.common.base.Optional
import org.bouncycastle.util.encoders.Hex
import org.embulk.filter.key_in_redis.column._

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import org.embulk.spi.time.TimestampFormatter
import org.embulk.spi.{Buffer, Exec, Page, PageBuilder, PageReader, Schema, PageOutput => EmbulkPageOutput}

case class PageOutput(task: PluginTask,
                      schema: Schema,
                      output: EmbulkPageOutput)
    extends EmbulkPageOutput {
  val pageBuilder = new PageBuilder(Exec.getBufferAllocator, schema, output)
  def timestampFormatter(): TimestampFormatter =
    new TimestampFormatter(task, Optional.absent())

  override def add(page: Page): Unit = {
    val reader: PageReader = new PageReader(schema)
    reader.setPage(page)
    val handlerBuffer = new ListBuffer[String]()
    while (reader.nextRecord()) {
      val setValueVisitor = SetValueColumnVisitor(
        reader,
        timestampFormatter(),
        task.getKeyWithIndex.asScala.toMap,
        task.getJsonKeyWithIndex.asScala.toMap,
        task.getAppender,
        task.getMatchAsMD5)
      schema.visitColumns(setValueVisitor)
      handlerBuffer.append(setValueVisitor.getValue)
    }

    KeyInRedisFilterPlugin.redis.foreach { redis =>
      val result = redis.exists(handlerBuffer)
      val newReader: PageReader = new PageReader(schema)
      newReader.setPage(page)
      while (newReader.nextRecord()) {
        val setValueVisitor = SetValueColumnVisitor(
          newReader,
          timestampFormatter(),
          task.getKeyWithIndex.asScala.toMap,
          task.getJsonKeyWithIndex.asScala.toMap,
          task.getAppender,
          task.getMatchAsMD5)
        schema.visitColumns(setValueVisitor)
        if (!result(setValueVisitor.getValue)) {
          val visitor = PassthroughColumnVisitor(newReader, pageBuilder)
          schema.visitColumns(visitor)
          visitor.addRecord()
        }
      }
    }
    reader.close()
  }

  override def finish(): Unit = pageBuilder.finish()
  override def close(): Unit = pageBuilder.close()

}

case class PageHandler(matchValue: String)

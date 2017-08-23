package org.embulk.filter.key_in_redis

import com.google.common.base.Optional
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

  override def add(page: Page): Unit = {
    val baseReader: PageReader = new PageReader(schema)
    baseReader.setPage(page)
    val rows = new ListBuffer[SetValueColumnVisitor]()
    while (baseReader.nextRecord()) {
      val setValueVisitor = SetValueColumnVisitor(
        baseReader,
        timestampFormatter(),
        task.getKeyWithIndex.asScala.toMap,
        task.getJsonKeyWithIndex.asScala.toMap,
        task.getAppender,
        task.getMatchAsMD5)
      schema.visitColumns(setValueVisitor)
      rows.append(setValueVisitor)
    }
    KeyInRedisFilterPlugin.redis.foreach { redis =>
      val result = redis.exists(rows.map(_.getMatchKey))
      rows.foreach { row =>
        if (!result(row.getMatchKey)) {
          row.addRecord(pageBuilder)
        }
      }
    }
  }

  override def finish(): Unit = pageBuilder.finish()
  override def close(): Unit = pageBuilder.close()

}
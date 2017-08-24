package org.embulk.filter.key_in_redis

import java.util.concurrent.TimeUnit

import com.google.common.base.Optional
import org.embulk.filter.key_in_redis.actor._
import org.embulk.filter.key_in_redis.row._
import org.embulk.filter.key_in_redis.ToFutureExtensionOps._
import akka.pattern.ask
import akka.util.Timeout

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
    while (baseReader.nextRecord()) {
      val setValueVisitor = SetValueColumnVisitor(
        baseReader,
        timestampFormatter(),
        task.getKeyWithIndex.asScala.toMap,
        task.getJsonKeyWithIndex.asScala.toMap,
        task.getAppender,
        task.getMatchAsMD5)
      schema.visitColumns(setValueVisitor)
      Actors.register ! setValueVisitor.getRow(pageBuilder)
    }
    baseReader.close()
  }

  override def finish(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val timeout = Timeout(24, TimeUnit.HOURS)
    (Actors.register ? Finish(pageBuilder))
      .mapTo[Applied]
      .toTask
      .unsafePerformSync

  }

  override def close(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val timeout = Timeout(24, TimeUnit.HOURS)
    (Actors.register ? Close(pageBuilder))
      .mapTo[Applied]
      .toTask
      .unsafePerformSync
  }

}

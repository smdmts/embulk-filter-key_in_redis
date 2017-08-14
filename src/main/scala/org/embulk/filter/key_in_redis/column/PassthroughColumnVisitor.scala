package org.embulk.filter.key_in_redis.column

import org.embulk.spi.{
  Column,
  PageBuilder,
  PageReader,
  ColumnVisitor => EmbulkColumnVisitor
}

case class PassthroughColumnVisitor(pageReader: PageReader,
                                    pageBuilder: PageBuilder)
    extends EmbulkColumnVisitor {

  override def timestampColumn(column: Column): Unit =
    if (pageReader.isNull(column)) {
      pageBuilder.setNull(column)
    } else {
      pageBuilder.setTimestamp(column, pageReader.getTimestamp(column))
    }

  override def stringColumn(column: Column): Unit =
    if (pageReader.isNull(column)) {
      pageBuilder.setNull(column)
    } else {
      pageBuilder.setString(column, pageReader.getString(column))
    }

  override def longColumn(column: Column): Unit =
    if (pageReader.isNull(column)) {
      pageBuilder.setNull(column)
    } else {
      pageBuilder.setLong(column, pageReader.getLong(column))
    }

  override def doubleColumn(column: Column): Unit =
    if (pageReader.isNull(column)) {
      pageBuilder.setNull(column)
    } else {
      pageBuilder.setDouble(column, pageReader.getDouble(column))
    }

  override def booleanColumn(column: Column): Unit =
    if (pageReader.isNull(column)) {
      pageBuilder.setNull(column)
    } else {
      pageBuilder.setBoolean(column, pageReader.getBoolean(column))
    }

  override def jsonColumn(column: Column): Unit =
    if (pageReader.isNull(column)) {
      pageBuilder.setNull(column)
    } else {
      pageBuilder.setJson(column, pageReader.getJson(column))
    }

  def addRecord(): Unit = pageBuilder.addRecord()
}

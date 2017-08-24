package org.embulk.filter.key_in_redis.row

import org.embulk.spi.Column

case class ValueHolder[A](column: Column, value: Option[A])

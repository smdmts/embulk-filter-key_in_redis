package org.embulk.filter.key_in_redis.redis

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scalaz._, Scalaz._
import scalaz.concurrent._

final class FutureExtensionOps[A](self: Future[A]) {
  def toTask(implicit ec: ExecutionContext): Task[A] = Task.async { register =>
    self.onComplete {
      case Success(v) => register(v.right)
      case Failure(ex) => register(ex.left)
    }
  }
}

trait ToFutureExtensionOps {
  implicit def toFutureExtensionOps[A](
      future: Future[A]): FutureExtensionOps[A] =
    new FutureExtensionOps(future)
}

object ToFutureExtensionOps extends ToFutureExtensionOps

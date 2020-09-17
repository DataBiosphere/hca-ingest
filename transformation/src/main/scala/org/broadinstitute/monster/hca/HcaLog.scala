package org.broadinstitute.monster.hca

import com.spotify.scio.ScioMetrics
import org.apache.beam.sdk.metrics.Counter
import org.slf4j.Logger
import ujson.Obj

// a simple log level case class hierarchy to make sure we log at the right level
trait LogLevel
case class HcaErrorLog() extends LogLevel
case class HcaWarnLog() extends LogLevel

// a trait to capture the generic logging mechanism we'll want, with case classes for the different logging levels
abstract class HcaLog {
  val level: LogLevel
  val jsonMsg: Obj
  val errorCount: Counter = ScioMetrics.counter("errorCount")

  def log(logger: Logger): Unit =
    level match {
      case HcaErrorLog() =>
        logger.error(jsonMsg.toString())
        errorCount.inc(1.toLong)
      case HcaWarnLog()  => logger.warn(jsonMsg.toString())
    }
}

class HcaWarn(msg: String) extends HcaLog {
  val level: HcaWarnLog = HcaWarnLog()

  val jsonMsg: Obj = ujson.Obj(
    "warningType" -> ujson.Str(this.getClass.getSimpleName),
    "message" -> ujson.Str(msg)
  )
}

class HcaError(filepath: String, msg: String) extends HcaLog {
  val level: HcaErrorLog = HcaErrorLog()

  // match everything that is not followed by a "/" (only the filename)
  private val filenameRegex = "[^/]*$".r
  private val filename = filenameRegex.findFirstIn(filepath).getOrElse("")

  val jsonMsg: Obj = ujson
    .Obj(
      "errorType" -> ujson.Str(this.getClass.getSimpleName),
      "filePath" -> ujson.Str(filepath),
      "fileName" -> ujson.Str(filename),
      "message" -> ujson.Str(msg)
    )
}

// case classes for all the different actual warnings and errors we want to raise during the workflow
case class NoMatchWarning(msg: String) extends HcaWarn(msg)

case class FileMismatchError(filepath: String, msg: String) extends HcaError(filepath, msg)

case class SchemaValidationError(filepath: String, msg: String) extends HcaError(filepath, msg)

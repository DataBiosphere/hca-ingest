package org.broadinstitute.monster.hca

import com.spotify.scio.ScioMetrics.counter
import org.broadinstitute.monster.hca.PostProcess.errCount
import org.slf4j.Logger
import ujson.Obj

// a trait to capture the generic logging mechanism we'll want, with case classes for the different logging levels
abstract class HcaLog {
  val jsonMsg: Obj

  def log(implicit logger: Logger): Unit
}

class HcaWarn(msg: String) extends HcaLog {
  def log(implicit logger: Logger): Unit = logger.warn(jsonMsg.toString())

  val jsonMsg: Obj = ujson.Obj(
    "warningType" -> ujson.Str(this.getClass.getSimpleName),
    "message" -> ujson.Str(msg)
  )
}

class HcaError(filepath: String, msg: String) extends HcaLog {

  def log(implicit logger: Logger): Unit = {
    logger.error(jsonMsg.toString())
    counter("main", errCount).inc()
  }

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

case class NoRegexPatternMatchError(filepath: String, msg: String) extends HcaError(filepath, msg)

case class MissingPropertyError(filepath: String, msg: String) extends HcaError(filepath, msg)

case class SchemaValidationError(filepath: String, msg: String) extends HcaError(filepath, msg)

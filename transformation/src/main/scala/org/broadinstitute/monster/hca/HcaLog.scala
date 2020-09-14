package org.broadinstitute.monster.hca

import org.slf4j.Logger
import ujson.Obj

// a simple log level case class heirarchy to make sure we log at the right level
trait LogLevel
case class HcaErrorLog() extends LogLevel
case class HcaWarnLog() extends LogLevel

// a trait to capture the generic logging mechanism we'll want, with case classes for the different logging levels
abstract class HcaLog {
  val level: LogLevel
  val jsonMsg: Obj

  def log(logger: Logger): Unit =
    level match {
      case HcaErrorLog() => logger.error(jsonMsg.toString())
      case HcaWarnLog()  => logger.warn(jsonMsg.toString())
    }
}

class HcaWarn(msg: String) extends HcaLog {
  val level: HcaWarnLog = HcaWarnLog()
  val jsonMsg: Obj = ujson.Obj(
    "type" -> ujson.Str(this.getClass.getName),
    "message" -> ujson.Str(msg)
  )
}

trait HcaError extends HcaLog {
  val level: HcaErrorLog = HcaErrorLog()
}

// case classes for all the different actual warnings and errors we want to raise during the workflow
case class DirectoryOrganizationWarning(msg: String) extends HcaWarn(msg)

case class EmptyDirectoryWarning(msg: String) extends HcaWarn(msg)

case class FileCountWarning(msg: String) extends HcaWarn(msg)

case class UniqueNameError(msg: String) extends HcaError {
  // TODO
  // In a given directory, are all .json files uniquely named?
}

case class FileMismatchError(msg: String) extends HcaError {
  // TODO
  // any nulls in metadata/{file_type} outer join descriptors/{file_type} outer join data?
}

case class SchemaValidationError(filepath: String, message: String) extends HcaError {
  private val filenameRegex = "[^/]*$".r // match everything that is not followed by a "/" (only the filename)
  private val filename = filenameRegex.findFirstIn(filepath).getOrElse("")

  val jsonMsg: Obj = ujson
    .Obj(
      "errorType" -> ujson.Str(this.getClass.getName),
      "filePath" -> ujson.Str(filepath),
      "fileName" -> ujson.Str(filename),
      "message" -> ujson.Str(message)
    )
}

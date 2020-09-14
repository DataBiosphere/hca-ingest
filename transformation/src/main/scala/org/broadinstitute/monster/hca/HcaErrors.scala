package org.broadinstitute.monster.hca

import org.slf4j.Logger


// a simple log level case class heirarchy to make sure we log at the right level
trait LogLevel
case class HcaErrorLog() extends LogLevel
case class HcaWarnLog() extends LogLevel

// a trait to capture the generic logging mechanism we'll want, with case classes for the different logging levels
trait HcaLog {
  val level: LogLevel
  val msg: String
  def log(logger: Logger): Unit = {
    level match {
      case HcaErrorLog() => logger.error(msg)
      case HcaWarnLog() => logger.warn(msg)
    }
  }
}
trait HcaWarn extends HcaLog {
  val level: HcaWarnLog = HcaWarnLog()
}
trait HcaError extends HcaLog {
  val level: HcaErrorLog = HcaErrorLog()
}

// case classes for all the different actual warnings and errors we want to raise during the workflow
case class DirectoryOrganizationWarning(msg: String) extends HcaWarn {
  // TODO
}

case class EmptyDirectoryWarning(msg: String) extends HcaWarn {
  // TODO
}

case class FileCountWarning(msg: String) extends HcaWarn {
  // TODO
}

case class UniqueNameError(msg: String) extends HcaError {
  // TODO
}

case class FileMismatchError(msg: String) extends HcaError {
  // TODO
}

case class SchemaValidationError(filepath: String, message: String) extends HcaError {
  private val filenameRegex = "[^/]*$".r // match everything that is not followed by a "/" (only the filename)
  private val filename = filenameRegex.findFirstIn(filepath).getOrElse("")
  val msg: String = ujson.Obj(
    "errorType" -> ujson.Str(this.getClass.getName),
    "filePath" -> ujson.Str(filepath),
    "fileName" -> ujson.Str(filename),
    "message" -> ujson.Str(message)
  ).toString()
}

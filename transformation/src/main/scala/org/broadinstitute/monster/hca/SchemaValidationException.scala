package org.broadinstitute.monster.hca

/** Exception thrown when we encounter a validation error while validating the schema. */
class SchemaValidationException(schemaSource: String, validationMessage: String) extends Exception {

  override def getMessage: String =
    s"Data does not conform to schema from $schemaSource; $validationMessage"

  override def
}
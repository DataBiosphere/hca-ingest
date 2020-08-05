package org.broadinstitute.monster.hca

import cats.data.Validated
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection

import io.circe.parser._
import io.circe.schema.Schema
import org.apache.beam.sdk.io.{FileIO, ReadableFileCoder}
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.slf4j.LoggerFactory
import ujson.StringRenderer
import upack.{Msg, Obj, Str}

import scala.util.{Failure, Success}
import scala.util.matching.Regex

object HcaPipelineBuilder extends PipelineBuilder[Args] {

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    val allMetadataEntities =
      metadataEntities.map(_ -> false) ++ fileMetadataEntities.map(_ -> true)
    allMetadataEntities.foreach {
      case (entityType, isFileMetadata) =>
        processMetadata(ctx, args.inputPrefix, args.outputPrefix, entityType, isFileMetadata)
    }
    processLinksMetadata(ctx, args.inputPrefix, args.outputPrefix)
    ()
  }

  private val logger = LoggerFactory.getLogger(getClass)

  implicit def coderSchema: Coder[Schema] = Coder.kryo[Schema]
  implicit val readableFileCoder: Coder[ReadableFile] = Coder.beam(new ReadableFileCoder)

  // format is: {entity_type}/{entity_id}_{version}.json,
  // but filename returns just {entity_id}_{version}.json, so that is what we deal with.
  val metadataPattern: Regex = "([^_]+)_(.+).json".r
  // format is: {dir_path}/{file_id}_{file_version}_{file_name},
  // but the dir_path seems to be optional
  val fileDataPattern: Regex = "(.*\\/)?([^_^\\/]+)_([^_]+)_(.+)".r
  // format is: {links_id}_{version}_{project_id}.json
  // but the `project_id` identifies the project the subgraph is part of
  // A subgraph is part of exactly one project. The importer must record an error if it
  // detects more than one object with the same `links/{links_id}_{version}_` prefix.
  val linksDataPattern: Regex = "([^_]+)_(.+)_([^_]+).json".r

  val metadataEntities = Set(
    "aggregate_generation_protocol",
    "analysis_process",
    "analysis_protocol",
    "cell_line",
    "cell_suspension",
    "collection_protocol",
    "differentiation_protocol",
    "dissociation_protocol",
    "donor_organism",
    "enrichment_protocol",
    "imaged_specimen",
    "imaging_preparation_protocol",
    "imaging_protocol",
    "ipsc_induction_protocol",
    "library_preparation_protocol",
    "organoid",
    "process",
    "project",
    "protocol",
    "sequencing_protocol",
    "specimen_from_organism"
  )

  val fileMetadataEntities = Set(
    "analysis_file",
    "image_file",
    "reference_file",
    "sequence_file",
    "supplementary_file"
  )

  /**
    * Given a pattern matching JSON, get the JSONs as ReadableFiles.
    *
    * @param jsonPath the root path containing JSONs to be converted
    * @param context context of the main V2F pipeline
    */
  def getReadableFiles(jsonPath: String, context: ScioContext): SCollection[FileIO.ReadableFile] =
    context.wrap {
      context.pipeline.apply(
        FileIO
          .`match`()
          .filepattern(jsonPath)
          .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD)
      )
    }.applyTransform[ReadableFile](FileIO.readMatches())

  /**
    * Given the SCollection of ReadableFiles that contains JSONs convert each JSON to a Msg and get its filename.
    *
    * @param tableName the name of the JSON table that was converted to Msg
    */
  def jsonToFilenameAndMsg(
    tableName: String
  ): SCollection[ReadableFile] => SCollection[(String, Msg)] =
    _.transform(s"Extract $tableName JSON rows") {
      _.map { file =>
        (
          file.getMetadata.resourceId.getFilename,
          JsonParser.parseEncodedJson(file.readFullyAsUTF8String)
        )
      }
    }

  /**
    *
    * Extract the necessary info from a metadata file and put it into a form that makes it easy to
    * pass in to the table format
    *
    * @param entityType the metadata entity type to prepend for the id field
    * @param fileName the raw filename of the metadata file
    * @param metadata the content of the metadata file in Msg format
    * @return a Msg object in the desired output format
    */
  def transformMetadata(entityType: String, fileName: String, metadata: Msg): Msg = {
    val (entityId, entityVersion) = getEntityIdAndVersion(fileName)
    Obj(
      Str(s"${entityType}_id") -> Str(entityId),
      Str("version") -> Str(entityVersion),
      Str("content") -> Str(encode(metadata))
    )
  }

  /**
    *
    * Extract the necessary info from a file of file-related metadata and put it into a form that
    * makes it easy to pass in to the table format
    *
    * @param entityType the metadata entity type to prepend for the id field
    * @param fileName the raw filename of the metadata file
    * @param metadata the content of the metadata file in Msg format
    * @param descriptor the content of the descriptor file in Msg format
    * @return a Msg object in the desired output format
    */
  def transformFileMetadata(
    entityType: String,
    fileName: String,
    metadata: Msg,
    descriptor: Msg
  ): Msg = {
    val (entityId, entityVersion) = getEntityIdAndVersion(fileName)
    // put values in the form we want
    Obj(
      Str(s"${entityType}_id") -> Str(entityId),
      Str("version") -> Str(entityVersion),
      Str("content") -> Str(encode(metadata)),
      Str("crc32c") -> descriptor.read[Msg]("crc32c"),
      Str("descriptor") -> Str(encode(descriptor))
    )
  }

  /**
    *
    * Extract the necessary info from a file of table relationships and put it into a form that
    * makes it easy to pass in to the table format
    *
    * @param fileName the raw filename of the metadata file
    * @param metadata the content of the metadata file in Msg format
    * @return a Msg object in the desired output format
    */
  def transformLinksFileMetadata(fileName: String, metadata: Msg): Msg = {
    val (linksId, linksVersion, projectId) = getLinksIdVersionAndProjectId(fileName)
    // put values in the form we want
    Obj(
      Str("content") -> Str(encode(metadata)),
      Str("links_id") -> Str(linksId),
      Str("version") -> Str(linksVersion),
      Str("project_id") -> Str(projectId)
    )
  }

  /**
    * Convert a file descriptor into a row in a bulk-file ingest request for the TDR.
    *
    * @param descriptor the content of a descriptor JSON file in Msg format
    * @param inputPrefix the root directory containing input files
    */
  def generateFileIngestRequest(
    descriptor: Msg,
    entityType: String,
    inputPrefix: String
  ): (String, Msg) = {
    val contentHash = descriptor.read[String]("crc32c")
    val targetPath = descriptor.read[String]("file_name")
    val sourcePath = s"$inputPrefix/data/$targetPath"

    contentHash -> Obj(
      Str("source_path") -> Str(sourcePath),
      Str("target_path") -> Str(s"/$entityType/$contentHash")
    )
  }

  /**
    * Extract the entity id and entity version from the name of a metadata file.
    *
    * @param fileName the raw filename of the metadata file
    * @return a tuple of the entity id and entity version
    */
  def getEntityIdAndVersion(fileName: String): (String, String) = {
    val matches = metadataPattern
      .findFirstMatchIn(fileName)
      .getOrElse(
        throw new Exception(
          s"transformMetadata: error when finding entity id and version from file named $fileName"
        )
      )
    val entityId = matches.group(1)
    val entityVersion = matches.group(2)
    (entityId, entityVersion)
  }

  /**
    * Extract the file id and file version from the name of a data file.
    *
    * @param fileName the raw filename of the data file
    * @return a tuple of the file id and file version
    */
  def getFileIdAndVersion(fileName: String): (String, String) = {
    val matches = fileDataPattern
      .findFirstMatchIn(fileName)
      .getOrElse(
        throw new Exception(
          s"transformMetadata: error when finding file id and version from file named $fileName"
        )
      )
    val fileId = matches.group(2)
    val fileVersion = matches.group(3)
    (fileId, fileVersion)
  }

  /**
    * Extract the links id, version, and project_id from the name of a "links.json" file.
    *
    * @param fileName the raw filename of the "links.json" file
    * @return a tuple of the links id, version, and project id
    */
  def getLinksIdVersionAndProjectId(fileName: String): (String, String, String) = {
    val matches = linksDataPattern
      .findFirstMatchIn(fileName)
      .getOrElse(
        throw new Exception(
          s"transformMetadata: error when finding links id, version, and project id from file named $fileName"
        )
      )
    val linksId = matches.group(1)
    val linksVersion = matches.group(2)
    val projectId = matches.group(3)
    (linksId, linksVersion, projectId)
  }

  /** Convert a Msg to a JSON string. */
  def encode(msg: Msg): String =
    upack.transform(msg, StringRenderer()).toString

  def logSchemaValidationError(filename: String, errorMessage: String): Unit = {
    val errorLog = ujson.Obj(
      "errorType" -> ujson.Str("SchemaValidationError"),
      "filePath" -> ujson.Str(""),
      "fileName" -> ujson.Str(filename),
      "message" -> ujson.Str(errorMessage)
    )
    logger.error(errorLog.toString())
  }

  def validateJson(filenamesAndMsg: SCollection[(String, Msg)]): SCollection[(String, Msg)] = {
    // pull out the url for where the schema definition is for each file
    val content = filenamesAndMsg.map {
      case (filename, msg) => (msg.read[String]("describedBy"), (filename, msg))
    }
    // get the distinct urls (so as to minimize the number of get requests) and then get the schemas as strings
    val schemas = content.map(_._1).distinct.map(url => (url, requests.get(url).text))
    // join the schemas to the data keyed by the schema url
    val joined = content.leftOuterJoin(schemas)
    // go over each row
    joined.map {
      case (url, ((filename, data), schemaOption)) => {
        // if there is nothing in the schemaOption, then something went wrong; if there is, try to validate
        schemaOption match {
          case Some(schema) => {
            // if the schema is not able to load, throw an exception, otherwise try to use it to validate
            Schema.loadFromString(schema) match {
              case Failure(_) => {
                val errorMessage = s"Schema not loaded properly for schema at $url, file $filename"
                logSchemaValidationError(filename, errorMessage)
                throw new Exception(errorMessage)
              }
              case Success(value) =>
                // try to parse the actual data into a json format for validation
                parse(encode(data)) match {
                  case Left(_) =>
                    val errorMessage = s"Unable to parse data into json for file $filename"
                    logSchemaValidationError(filename, errorMessage)
                    throw new Exception(errorMessage)
                  // if everything is parsed/encoded/etc correctly, actually try to validate against schema here
                  // if not valid, will return list of issues
                  case Right(success) => {
                    value.validate(success) match {
                      case Validated.Valid(_) => (filename, data)
                      case Validated.Invalid(e) => {
                        val errorMessage =
                          s"Data does not conform to schema from $url; ${e.map(_.getMessage).toList.mkString(",")}"
                        logSchemaValidationError(filename, errorMessage)
                        throw new Exception(errorMessage)
                      }
                    }
                  }
                }
            }
          }
          val errorMessage = s"No schema found at $url for file $filename"
          logSchemaValidationError(filename, errorMessage)
          throw new Exception(errorMessage)
        }
      }
    }
  }

  /**
    * Read, transform, and write a given entity type.
    *
    * @param context context of the main pipeline
    * @param inputPrefix the root directory containing JSONs to be converted
    * @param outputPrefix the directory to write outputs to
    * @param entityType the bucket name/table name to read/write from
    */
  def processMetadata(
    context: ScioContext,
    inputPrefix: String,
    outputPrefix: String,
    entityType: String,
    isFileMetadata: Boolean = false
  ): ClosedTap[String] = {
    // get the readable files for the given input path
    val metadataFiles = getReadableFiles(s"$inputPrefix/metadata/${entityType}/**.json", context)
    val metadataFilenameAndMsg = jsonToFilenameAndMsg(entityType)(metadataFiles)

    val validatedFilenameAndMsg = validateJson(metadataFilenameAndMsg)

    // for file metadata
    val processedMetadata = if (isFileMetadata) {
      val descriptorFiles = getReadableFiles(
        s"$inputPrefix/descriptors/$entityType/**.json",
        context
      )
      val descriptorFilenameAndMsg = jsonToFilenameAndMsg(entityType)(descriptorFiles)
      val processedFileMetadata = validatedFilenameAndMsg
        .join(descriptorFilenameAndMsg)
        .withName(s"Pre-process $entityType metadata")
        .map {
          case (filename, (metadata, descriptor)) =>
            transformFileMetadata(entityType, filename, metadata, descriptor)
        }

      // Generate file ingest requests from descriptors. Deduplicate by the content hash.
      val fileIngestRequests = descriptorFilenameAndMsg.map {
        case (_, descriptor) => generateFileIngestRequest(descriptor, entityType, inputPrefix)
      }.distinctByKey.values
      StorageIO.writeJsonLists(
        fileIngestRequests,
        entityType,
        s"${outputPrefix}/data-transfer-requests/${entityType}"
      )
      processedFileMetadata
    } else {
      // for non-file metadata
      validatedFilenameAndMsg
        .withName(s"Pre-process $entityType metadata")
        .map {
          case (filename, metadata) => transformMetadata(entityType, filename, metadata)
        }
    }
    // then write to storage
    StorageIO.writeJsonLists(
      processedMetadata,
      entityType,
      s"${outputPrefix}/metadata/${entityType}"
    )
  }

  def processLinksMetadata(
    context: ScioContext,
    inputPrefix: String,
    outputPrefix: String
  ): ClosedTap[String] = {
    // get the readable files for the given input path
    val readableFiles = getReadableFiles(
      s"$inputPrefix/links/**.json",
      context
    )
    val linksMetadataFilenameAndMsg = jsonToFilenameAndMsg("links")(readableFiles)

    val validatedFilenameAndMsg = validateJson(linksMetadataFilenameAndMsg)

    // then convert json to msg and get the filename
    val processedData = validatedFilenameAndMsg
      .withName(s"Pre-process links metadata")
      .map {
        case (filename, metadata) =>
          transformLinksFileMetadata(filename, metadata)
      }
    // then write to storage
    StorageIO.writeJsonLists(
      processedData,
      "links",
      s"${outputPrefix}/metadata/links"
    )
  }
}

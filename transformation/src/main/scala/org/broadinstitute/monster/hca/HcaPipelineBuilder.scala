package org.broadinstitute.monster.hca

import cats.data.Validated
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import io.circe.parser._
import io.circe.schema.Schema
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.fs.{EmptyMatchTreatment, MatchResult}
import org.apache.beam.sdk.io.{FileIO, ReadableFileCoder}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.slf4j.{Logger, LoggerFactory}
import ujson.StringRenderer
import upack.{Msg, Obj, Str}

import scala.util.matching.Regex
import scala.util.{Failure, Success}

object HcaPipelineBuilder extends PipelineBuilder[Args] {

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    // are all top level dirs present? If not, which ones are absent? raise warns
    // are all the right entities under metadata and descriptors present? If not, which ones are absent? raise warns
    // Is the count of metadata/{file_type} == descriptors/{file_type}? if not, raise warns
    // Is the count of data/** == metadata/** == descriptors/**? if not, raise warns

    val dataFiles =
      matchFiles(s"${args.inputPrefix}/data/**", ctx).keyBy(_.resourceId().getFilename)

    val allMetadataEntities =
      expectedMetadataEntities.map(_ -> false) ++ expectedFileEntities.map(_ -> true)
    allMetadataEntities.foreach {
      case (entityType, isFileMetadata) =>
        processMetadata(
          ctx,
          args.inputPrefix,
          args.outputPrefix,
          entityType,
          dataFiles,
          isFileMetadata
        )
    }
    processLinksMetadata(ctx, args.inputPrefix, args.outputPrefix)
    ()
  }

  implicit val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val coder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

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
  // grab everything after the last "/"
  val fileNamePattern: Regex = "([^/]+$)".r

  val expectedTopLevelDirs = Set(
    "data",
    "descriptors",
    "links",
    "metadata"
  )

  val expectedMetadataEntities = Set(
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

  val expectedFileEntities = Set(
    "analysis_file",
    "image_file",
    "reference_file",
    "sequence_file",
    "supplementary_file"
  )

  /**
    * Given a pattern match, get the file MatchResult.Metadata.
    *
    * @param filePattern the root path containing files to be matched
    * @param context context of the main pipeline
    */
  def matchFiles(filePattern: String, context: ScioContext): SCollection[MatchResult.Metadata] = {
    val metadata = context.wrap {
      context.pipeline.apply(
        FileIO
          .`match`()
          .filepattern(filePattern)
          .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD)
      )
    }
    metadata.count
      .map(c => if (c == 0.toLong) NoMatchWarning(s"$filePattern had $c matches").log)
    metadata
  }

  /**
    * Given a pattern matching JSON, get the JSONs as ReadableFiles.
    *
    * @param jsonPath the root path containing JSONs to be converted
    * @param context context of the main pipeline
    */
  def getReadableFiles(jsonPath: String, context: ScioContext): SCollection[FileIO.ReadableFile] = {
    val metadata = matchFiles(jsonPath, context)
    metadata.applyTransform[ReadableFile](FileIO.readMatches())
  }

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
    * Extract the necessary info from a metadata file and put it into a form that makes it easy to
    * pass in to the table format
    *
    * @param entityType the metadata entity type to prepend for the id field
    * @param fileName the raw filename of the metadata file
    * @param metadata the content of the metadata file in Msg format
    * @return a Msg object in the desired output format
    */
  def transformMetadata(
    entityType: String,
    fileName: String,
    metadata: Msg,
    inputPrefix: String
  ): Option[Msg] =
    getEntityIdAndVersion(fileName, entityType, inputPrefix) match {
      case Some((entityId, entityVersion)) =>
        Some(
          Obj(
            Str(s"${entityType}_id") -> Str(entityId),
            Str("version") -> Str(entityVersion),
            Str("content") -> Str(encode(metadata))
          )
        )
      case None => None
    }

  /**
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
    descriptor: Msg,
    inputPrefix: String
  ): Option[Msg] =
    getEntityIdAndVersion(fileName, entityType, inputPrefix) match {
      case Some((entityId, entityVersion)) =>
        Some(
          Obj(
            Str(s"${entityType}_id") -> Str(entityId),
            Str("version") -> Str(entityVersion),
            Str("content") -> Str(encode(metadata)),
            Str("crc32c") -> descriptor.tryRead[Msg]("crc32c").getOrElse {
              MissingPropertyError(
                s"$inputPrefix/metadata/$entityType/$fileName",
                s"Descriptor for file $fileName has no crc32c property."
              ).log
              Str("")
            },
            Str("descriptor") -> Str(encode(descriptor))
          )
        )
      case None => None
    }

  /**
    * Extract the necessary info from a file of table relationships and put it into a form that
    * makes it easy to pass in to the table format
    *
    * @param fileName the raw filename of the metadata file
    * @param metadata the content of the metadata file in Msg format
    * @return a Msg object in the desired output format
    */
  def transformLinksFileMetadata(
    fileName: String,
    metadata: Msg,
    inputPrefix: String
  ): Option[Msg] =
    getLinksIdVersionAndProjectId(fileName, inputPrefix) match {
      case Some((linksId, linksVersion, projectId)) =>
        Some(
          Obj(
            Str("content") -> Str(encode(metadata)),
            Str("links_id") -> Str(linksId),
            Str("version") -> Str(linksVersion),
            Str("project_id") -> Str(projectId)
          )
        )
      case None => None
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
    inputPrefix: String,
    filename: String
  ): Option[(String, Msg)] = {
    val totalPath = s"$inputPrefix/descriptors/$entityType/$filename"
    val targetPath = descriptor.tryRead[String]("file_name").getOrElse {
      MissingPropertyError(totalPath, s"Descriptor file $filename has no file_name property.").log
      ""
    }
    val sourcePath = s"$inputPrefix/data/$targetPath"
    val contentHash = descriptor.tryRead[String]("crc32c").getOrElse {
      MissingPropertyError(totalPath, s"Descriptor file $filename has no crc32c property.").log
      ""
    }

    val matches = fileNamePattern
      .findFirstMatchIn(targetPath)
      .getOrElse(
        NoRegexPatternMatchError(
          totalPath,
          s"Could not parse filename for file ingest request creation."
        )
      )

    matches match {
      case valid: Regex.Match =>
        Some(
          contentHash -> Obj(
            Str("source_path") -> Str(sourcePath),
            Str("target_path") -> Str(s"/$entityType/${valid.group(1)}")
          )
        )
      case err: HcaError =>
        err.log
        None
    }

  }

  /**
    * Extract the entity id and entity version from the name of a metadata file.
    *
    * @param fileName the raw filename of the metadata file
    * @return a tuple of the entity id and entity version
    */
  def getEntityIdAndVersion(
    fileName: String,
    inputPrefix: String,
    entityType: String
  ): Option[(String, String)] = {
    val matches = metadataPattern
      .findFirstMatchIn(fileName)
      .getOrElse(
        NoRegexPatternMatchError(
          s"$inputPrefix/metadata/$entityType/$fileName",
          s"Error when finding entity id and version from file named $fileName"
        )
      )

    matches match {
      case valid: Regex.Match =>
        val entityId = valid.group(1)
        val entityVersion = valid.group(2)
        Some((entityId, entityVersion))
      case err: HcaError =>
        err.log
        None
    }
  }

  /**
    * Extract the links id, version, and project_id from the name of a "links.json" file.
    *
    * @param fileName the raw filename of the "links.json" file
    * @return a tuple of the links id, version, and project id
    */
  def getLinksIdVersionAndProjectId(
    fileName: String,
    inputPrefix: String
  ): Option[(String, String, String)] = {
    val matches = linksDataPattern
      .findFirstMatchIn(fileName)
      .getOrElse(
        NoRegexPatternMatchError(
          s"$inputPrefix/links/$fileName",
          s"Error when finding links id, version, and project id from file named $fileName"
        )
      )

    matches match {
      case valid: Regex.Match =>
        val linksId = valid.group(1)
        val linksVersion = valid.group(2)
        val projectId = valid.group(3)
        Some((linksId, linksVersion, projectId))
      case err: HcaError =>
        err.log
        None
    }
  }

  /** Convert a Msg to a JSON string. */
  def encode(msg: Msg): String =
    upack.transform(msg, StringRenderer()).toString

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
    files: SCollection[(String, MatchResult.Metadata)],
    isFileMetadata: Boolean = false
  ): ClosedTap[String] = {
    // get the readable files for the given input path
    val metadataFiles = getReadableFiles(s"$inputPrefix/metadata/$entityType/**.json", context)
    val metadataFilenameAndMsg = jsonToFilenameAndMsg(entityType)(metadataFiles)

    val validatedFilenameAndMsg =
      validateJson(metadataFilenameAndMsg, s"$inputPrefix/metadata/$entityType")

    // for file metadata
    val processedMetadata = if (isFileMetadata) {
      val descriptorFiles = getReadableFiles(
        s"$inputPrefix/descriptors/$entityType/**.json",
        context
      )
      val descriptorFilenameAndMsg = jsonToFilenameAndMsg(entityType)(descriptorFiles)
      val validatedDescriptors =
        validateJson(descriptorFilenameAndMsg, s"$inputPrefix/descriptors/$entityType")
      val processedFileMetadata = validatedFilenameAndMsg
        .fullOuterJoin(validatedDescriptors)
        .withName(s"Pre-process $entityType metadata")
        .flatMap {
          // file is present in metadata but not descriptors
          case (filename, (Some(_), None)) =>
            val err = FileMismatchError(
              s"$inputPrefix/descriptors/$entityType/$filename",
              s"$filename is present in metadata/$entityType but not in descriptors/$entityType"
            )
            err.log
            None
          // file is present in descriptors but not metadata
          case (filename, (None, Some(_))) =>
            val err = FileMismatchError(
              s"$inputPrefix/metadata/$entityType/$filename",
              s"$filename is present in descriptors/$entityType but not in metadata/$entityType"
            )
            err.log
            None
          // file is present in both, only valid case to continue on
          case (filename, (Some(metadata), Some(descriptor))) =>
            transformFileMetadata(entityType, filename, metadata, descriptor, inputPrefix)
        }

      // Generate file ingest requests from descriptors. Deduplicate by the content hash.
      val keyedIngestRequests = validatedDescriptors.flatMap {
        case (filename, descriptor) =>
          generateFileIngestRequest(descriptor, entityType, inputPrefix, filename)
      }.distinctByKey.values.keyBy(request => request.read[String]("source_path"))

      val fileIngestRequests = keyedIngestRequests.leftOuterJoin(files).flatMap {
        case (filename, (_, None)) =>
          FileMismatchError(
            s"$inputPrefix/data/$filename",
            s"$filename has a descriptors/$entityType and metadata/$entityType but doesn't actually exist under data/"
          ).log
          None
        case (_, (request, Some(_))) => Some(request)
      }

      StorageIO.writeJsonLists(
        fileIngestRequests,
        entityType,
        s"$outputPrefix/data-transfer-requests/$entityType"
      )
      processedFileMetadata
    } else {
      // for non-file metadata
      validatedFilenameAndMsg
        .withName(s"Pre-process $entityType metadata")
        .flatMap {
          case (filename, metadata) =>
            transformMetadata(entityType, filename, metadata, inputPrefix)
        }
    }
    // then write to storage
    StorageIO.writeJsonLists(
      processedMetadata,
      entityType,
      s"$outputPrefix/metadata/$entityType"
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

    val validatedFilenameAndMsg = validateJson(linksMetadataFilenameAndMsg, s"$inputPrefix/links")

    // then convert json to msg and get the filename
    val processedData = validatedFilenameAndMsg
      .withName(s"Pre-process links metadata")
      .flatMap {
        case (filename, metadata) =>
          transformLinksFileMetadata(filename, metadata, inputPrefix)
      }
    // then write to storage
    StorageIO.writeJsonLists(
      processedData,
      "links",
      s"$outputPrefix/metadata/links"
    )
  }

  def validateJson(
    filenamesAndMsg: SCollection[(String, Msg)],
    inputPrefix: String
  ): SCollection[(String, Msg)] = {
    validateJsonInternal(inputPrefix)(filenamesAndMsg).withName("Validate: Log Validation").map {
      case Some(error) =>
        error.log
      case None =>
    }
    filenamesAndMsg
  }

  def validateJsonInternal(inputPrefix: String)(
    filenamesAndMsg: SCollection[(String, Msg)]
  ): SCollection[Option[SchemaValidationError]] = {
    // pull out the url for where the schema definition is for each file
    val content = filenamesAndMsg.withName("Validate: Schema Definition URL").map {
      case (filename, msg) =>
        (
          msg.tryRead[String]("describedBy").getOrElse {
            MissingPropertyError(
              s"$inputPrefix/$filename",
              s"File $filename has no describedBy property."
            ).log
            ""
          },
          (filename, msg)
        )
    }
    // get the distinct urls (so as to minimize the number of get requests) and then get the schemas as strings
    val schemas = content
      .withName("Validate: Schema Content URL")
      .map(_._1)
      .filter(!_.isEmpty)
      .distinct
      .withName("Validate: Schema Content")
      .map(url => (url, requests.get(url).text))
    // join the schemas to the data keyed by the schema url
    val joined = content.leftOuterJoin(schemas)
    // go over each row
    joined.withName("Validate: Metadata files against schema definition").map {
      case (url, ((filename, data), schemaOption)) =>
        val errPath = s"$inputPrefix/$filename"
        // if there is nothing in the schemaOption, then something went wrong; if there is, try to validate
        schemaOption match {
          case Some(schema) =>
            // if the schema is not able to load, log an error, otherwise try to use it to validate
            Schema.loadFromString(schema) match {
              case Failure(_) =>
                Option(
                  SchemaValidationError(
                    errPath,
                    s"Schema not loaded properly for schema at $url, file $filename"
                  )
                )
              case Success(value) =>
                // try to parse the actual data into a json format for validation
                parse(encode(data)) match {
                  case Left(_) =>
                    Option(
                      SchemaValidationError(
                        errPath,
                        s"Unable to parse data into json for file $filename"
                      )
                    )
                  // if everything is parsed/encoded/etc correctly, actually try to validate against schema here
                  // if not valid, will return list of issues
                  case Right(success) =>
                    value.validate(success) match {
                      case Validated.Valid(_) => None
                      case Validated.Invalid(e) =>
                        Option(
                          SchemaValidationError(
                            errPath,
                            s"Data in file $filename does not conform to schema from $url; ${e.map(_.getMessage).toList.mkString(",")}"
                          )
                        )
                    }
                }
            }
          case None =>
            Option(SchemaValidationError(errPath, s"No schema found at $url for file $filename"))
        }
    }
  }
}

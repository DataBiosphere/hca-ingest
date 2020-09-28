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

import scala.util.{Failure, Success}

object HcaPipelineBuilder extends PipelineBuilder[Args] {

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    // are all top level dirs present? If not, which ones are absent? raise warns
    // are all the right entities under metadata and descriptors present? If not, which ones are absent? raise warns
    // Is the count of metadata/{file_type} == descriptors/{file_type}? if not, raise warns
    // Is the count of data/** == metadata/** == descriptors/**? if not, raise warns

    // need to include the input prefix here so that a join can work later for validation
    val dataFiles =
      matchFiles(s"${args.inputPrefix}/data/**", ctx).keyBy(_.resourceId().getFilename).map {
        case (filename, rest) => (s"${args.inputPrefix}/data/$filename", rest)
      }

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
      .map(c => if (c == 0) NoMatchWarning(s"$filePattern had no matches").log)
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
    getEntityIdAndVersion(fileName, inputPrefix, entityType).map {
      case (entityId, entityVersion) =>
        Obj(
          Str(s"${entityType}_id") -> Str(entityId),
          Str("version") -> Str(entityVersion),
          Str("content") -> Str(encode(metadata))
        )
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
    getEntityIdAndVersion(fileName, inputPrefix, entityType).map {
      case (entityId, entityVersion) =>
        Obj(
          Str(s"${entityType}_id") -> Str(entityId),
          Str("version") -> Str(entityVersion),
          Str("content") -> Str(encode(metadata)),
          Str("crc32c") -> descriptor.read[Msg]("crc32c"),
          Str("descriptor") -> Str(encode(descriptor))
        )
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
    getLinksIdVersionAndProjectId(fileName, inputPrefix).map {
      case (linksId, linksVersion, projectId) =>
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
    inputPrefix: String,
    filename: String
  ): Option[(String, Msg)] = {
    val totalPath = s"$inputPrefix/descriptors/$entityType/$filename"
    val targetPath = descriptor
      .tryRead[String]("file_name")
      .fold[Option[String]] {
        MissingPropertyError(totalPath, "Descriptor file has no file_name property.").log
        None
      }(Some(_))
    val contentHash = descriptor
      .read[String]("crc32c")

    targetPath.map { tpath =>
      contentHash -> Obj(
        Str("source_path") -> Str(s"$inputPrefix/data/$tpath"),
        Str("target_path") -> Str(s"/$contentHash/${tpath.substring(tpath.lastIndexOf("/") + 1)}")
      )
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
    val parts = fileName.stripSuffix(".json").split("_")

    if (parts.length != 2) {
      NoRegexPatternMatchError(
        s"$inputPrefix/metadata/$entityType/$fileName",
        "Error when finding entity id and version from file name."
      ).log
      None
    } else {
      Some((parts(0), parts(1)))
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
    val parts = fileName.stripSuffix(".json").split("_")

    if (parts.length != 3) {
      NoRegexPatternMatchError(
        s"$inputPrefix/links/$fileName",
        "Error when finding links id, version, and project id from file."
      ).log
      None
    } else {
      Some((parts(0), parts(1), parts(2)))
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
      val descriptorFilenameAndMsg = jsonToFilenameAndMsg(entityType)(descriptorFiles).filter {
        case (filename, descriptor) =>
          descriptor
            .tryRead[String]("crc32c")
            .fold {
              MissingPropertyError(
                s"$inputPrefix/descriptors/$entityType/$filename",
                s"Descriptor file has no crc32c property."
              ).log
              false
            }(_ => true)
      }
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
              s"File is present in metadata/$entityType but not in descriptors/$entityType."
            )
            err.log
            None
          // file is present in descriptors but not metadata
          case (filename, (None, Some(_))) =>
            val err = FileMismatchError(
              s"$inputPrefix/metadata/$entityType/$filename",
              s"File is present in descriptors/$entityType but not in metadata/$entityType."
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

      val fileIngestRequests =
        keyedIngestRequests.leftOuterJoin(files).flatMap {
          case (filename, (_, None)) =>
            FileMismatchError(
              filename,
              s"File has a descriptors/$entityType and metadata/$entityType but doesn't actually exist under data/."
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
    val content = filenamesAndMsg
      .withName("Validate: Schema Definition URL")
      .map {
        case (filename, msg) =>
          (
            msg
              .tryRead[String]("describedBy")
              .fold[Option[String]] {
                MissingPropertyError(
                  s"$inputPrefix/$filename",
                  "File has no describedBy property."
                ).log
                None
              }(Some(_)),
            (filename, msg)
          )
      }
      .collect { case (Some(url), rest) => (url, rest) }
    // get the distinct urls (so as to minimize the number of get requests) and then get the schemas as strings
    val schemas = content
      .withName("Validate: Schema Content URL")
      .map(_._1)
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
                    s"Schema not loaded properly for schema at $url"
                  )
                )
              case Success(value) =>
                // try to parse the actual data into a json format for validation
                parse(encode(data)) match {
                  case Left(_) =>
                    Option(
                      SchemaValidationError(
                        errPath,
                        "Unable to parse data into json for file"
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
                            s"Data in file does not conform to schema from $url; ${e.map(_.getMessage).toList.mkString(",")}"
                          )
                        )
                    }
                }
            }
          case None =>
            Option(SchemaValidationError(errPath, s"No schema found at $url"))
        }
    }
  }
}

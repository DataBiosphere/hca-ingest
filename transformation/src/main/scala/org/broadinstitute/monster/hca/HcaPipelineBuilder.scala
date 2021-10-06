package org.broadinstitute.monster.hca

import cats.data.Validated
import com.spotify.scio.{ScioContext, ScioMetrics}
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
import org.broadinstitute.monster.hca.PostProcess.errCount
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

    ScioMetrics.counter("main", errCount)

    val allMetadataEntities =
      expectedMetadataEntities.map(_ -> false) ++ expectedFileEntities.map(_ -> true)
    allMetadataEntities.foreach {
      case (entityType, isFileMetadata) =>
        processMetadata(
          ctx,
          args.inputPrefix,
          args.outputPrefix,
          entityType,
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
    inputPrefix: String
  ): (String, Msg) = {
    val fileName = descriptor
      .read[String]("file_name")
    val contentId = descriptor
      .read[String]("file_id")

    val targetPath =
      s"/$contentId/${descriptor.read[String]("crc32c")}/${fileName.substring(fileName.lastIndexOf("/") + 1)}"

    contentId -> Obj(
      Str("source_path") -> Str(s"$inputPrefix/data/$fileName"),
      Str("target_path") -> Str(targetPath)
    )
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
    * @param filename the filename to use if the property is missing
    * @param msg the message body to check the property for
    * @param property the property in the JSON to check and filter on
    * @param inputPrefix entire prefix to go before the file name for error logging
    * @return A boolean to use in a filter
    */
  def filterProperty(filename: String, msg: Msg, property: String, inputPrefix: String): Boolean = {
    msg
      .tryRead[String](property)
      .fold {
        MissingPropertyError(
          s"$inputPrefix/$filename",
          s"File has no $property property."
        ).log
        false
      }(_ => true)
  }

  /**
    * @param descriptor Filename and content of a descriptor file
    * @param inputPrefix The high-level input prefix, so the bucket and path before /descriptors
    * @param entityType The type of entity, which for descriptors is limited to the file types
    * @return A boolean to use in a filter
    */
  def filterDescriptor(
    descriptor: (String, Msg),
    inputPrefix: String,
    entityType: String
  ): Boolean = {
    val prefix = s"$inputPrefix/descriptors/$entityType"
    descriptor match {
      case (filename, msg) =>
        filterProperty(filename, msg, "crc32c", prefix) &&
          filterProperty(filename, msg, "file_name", prefix)
    }
  }

  /**
    * Read, transform, and write a given entity type.
    *
    * @param context context of the main pipeline
    * @param inputPrefix the root directory containing JSONs to be converted
    * @param outputPrefix the directory to write outputs to
    * @param entityType the bucket name/table name to read/write from
    * @param isFileMetadata whether or not the input is for file metadata
    */
  def processMetadata(
    context: ScioContext,
    inputPrefix: String,
    outputPrefix: String,
    entityType: String,
    isFileMetadata: Boolean = false
  ): ClosedTap[String] = {
    // get the readable files for the given input path
    val metadataFiles = getReadableFiles(s"$inputPrefix/metadata/$entityType/**.json", context)
    val metadataFilenameAndMsg = jsonToFilenameAndMsg(entityType)(metadataFiles)

    val validatedFilenameAndMsg =
      validateJson(s"$inputPrefix/metadata/$entityType")(metadataFilenameAndMsg)

    // for file metadata
    val processedMetadata = if (isFileMetadata) {
      val descriptorFiles = getReadableFiles(
        s"$inputPrefix/descriptors/$entityType/**.json",
        context
      )
      val descriptorFilenameAndMsg = jsonToFilenameAndMsg(entityType)(descriptorFiles)
        .filter(filterDescriptor(_, inputPrefix, entityType))
      val validatedDescriptors =
        validateJson(s"$inputPrefix/descriptors/$entityType")(descriptorFilenameAndMsg)
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
      val fileIngestRequests = validatedDescriptors.map {
        case (_, descriptor) =>
          generateFileIngestRequest(descriptor, inputPrefix)
      }.distinctByKey.values

      StorageIO.writeJsonListsGeneric(
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
    StorageIO.writeJsonListsGeneric(
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

    val validatedFilenameAndMsg = validateJson(s"$inputPrefix/links")(linksMetadataFilenameAndMsg)

    // then convert json to msg and get the filename
    val processedData = validatedFilenameAndMsg
      .withName(s"Pre-process links metadata")
      .flatMap {
        case (filename, metadata) =>
          transformLinksFileMetadata(filename, metadata, inputPrefix)
      }
    // then write to storage
    StorageIO.writeJsonListsGeneric(
      processedData,
      "links",
      s"$outputPrefix/metadata/links"
    )
  }

  /**
    * @param inputPrefix The entire prefix before the filename
    * @param filenamesAndMsg The filenames and messages to validate
    * @return A collection of possible errors
    */
  def validateJson(inputPrefix: String)(
    filenamesAndMsg: SCollection[(String, Msg)]
  ): SCollection[(String, Msg)] = {
    val urlAndFilenameWithMsg = filenamesAndMsg.filter {
      case (filename, msg) => filterProperty(filename, msg, "describedBy", inputPrefix)
    }.keyBy { case (_, msg) => msg.read[String]("describedBy") }

    // get the distinct urls (so as to minimize the number of get requests) and then get the schemas as strings
    val schemas = urlAndFilenameWithMsg
      .withName("Validate: Schema Content URL")
      .map(_._1)
      .distinct
      .withName("Validate: Schema Content")
      .map(url => (url, requests.get(url).text))
    // join the schemas to the data keyed by the schema url
    val joined = urlAndFilenameWithMsg.leftOuterJoin(schemas)
    // go over each row
    joined.withName("Validate: Metadata files against schema definition").map {
      case (url, ((filename, data), schemaOption)) =>
        val errPath = s"$inputPrefix/$filename"
        // if there is nothing in the schemaOption, then something went wrong; if there is, try to validate
        val outOption = schemaOption match {
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
        outOption.foreach { _.log }
    }
    filenamesAndMsg
  }
}

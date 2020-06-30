package org.broadinstitute.monster.hca

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.{FileIO, ReadableFileCoder}
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import ujson.StringRenderer
import upack.{Msg, Obj, Str}

import scala.collection.mutable
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
      mutable.LinkedHashMap[Msg, Msg](
        Str(s"${entityType}_id") -> Str(entityId),
        Str("version") -> Str(entityVersion),
        Str("content") -> Str(encode(metadata))
      )
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
    val dataFileName = descriptor.read[String]("file_name")
    // put values in the form we want
    Obj(
      mutable.LinkedHashMap[Msg, Msg](
        Str(s"${entityType}_id") -> Str(entityId),
        Str("version") -> Str(entityVersion),
        Str("content") -> Str(encode(metadata)),
        Str("virtual_path") -> Str(s"/$dataFileName"),
        Str("crc32c") -> descriptor.read[Msg]("crc32c"),
        Str("descriptor") -> Str(encode(descriptor))
      )
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
      mutable.LinkedHashMap[Msg, Msg](
        Str("content") -> Str(encode(metadata)),
        Str("links_id") -> Str(linksId),
        Str("version") -> Str(linksVersion),
        Str("project_id") -> Str(projectId)
      )
    )
  }

  /**
    * Extract the necessary info from file metadata and put it into a form that
    * can be used to generate bulk file ingest requests.
    * @param metadata the content of the metadata file in Msg format
    * @param inputPrefix the root directory containing input files
    * @return a Msg object in the desired output format
    */
  def generateFileIngestRequest(metadata: Msg, inputPrefix: String): Msg = {
    val contentHash = metadata.read[String]("crc32c")
    val targetPath = metadata.read[String]("virtual_path")
    val sourcePath = s"$inputPrefix/data$targetPath"

    Obj(
      mutable.LinkedHashMap[Msg, Msg](
        Str("source_path") -> Str(sourcePath),
        Str("target_path") -> Str(targetPath),
        Str("crc32c") -> Str(contentHash)
      )
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

    // for file metadata
    val processedMetadata = if (isFileMetadata) {
      val descriptorFiles = getReadableFiles(
        s"$inputPrefix/descriptors/$entityType/**.json",
        context
      )
      val descriptorFilenameAndMsg = jsonToFilenameAndMsg(entityType)(descriptorFiles)
      val processedFileMetadata = metadataFilenameAndMsg
        .join(descriptorFilenameAndMsg)
        .withName(s"Pre-process $entityType metadata")
        .map {
          case (filename, (metadata, descriptor)) =>
            transformFileMetadata(entityType, filename, metadata, descriptor)
        }
      // generate file ingest request
      val fileIngestRequests = processedFileMetadata.map(generateFileIngestRequest(_, inputPrefix))
      StorageIO.writeJsonLists(
        fileIngestRequests,
        entityType,
        s"${outputPrefix}/data-transfer-requests/${entityType}"
      )
      processedFileMetadata
    } else {
      // for non-file metadata
      metadataFilenameAndMsg
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
    // then convert json to msg and get the filename
    val processedData =
      jsonToFilenameAndMsg("links")(readableFiles)
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

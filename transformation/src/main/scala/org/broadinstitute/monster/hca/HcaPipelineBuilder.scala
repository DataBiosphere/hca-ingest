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
    // process metadata for each non-file metadata entity
    metadataEntities.foreach { entityType =>
      processMetadata(ctx, args.inputPrefix, args.outputPrefix, entityType)
    }
    ()
  }

  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)
  implicit val readableFileCoder: Coder[ReadableFile] = Coder.beam(new ReadableFileCoder)

  // format is: metadata/{entity_type}/{entity_id}_{version}.json,
  // but filename returns just {entity_id}_{version}.json, so that is what we deal with.
  val metadataPattern: Regex = "([^_]+)_(.+).json".r
  // format is: {dir_path}{file_id}_{file_version}_{file_name},
  // but the dir_path seems to be optional
  val fileDataPattern: Regex = "(.*\\/)?([^_^\\/]+)_([^_]+)_(.+)".r

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
    "image_specimen",
    "imaging_preperation_protocol",
    "imaging_protocol",
    "ipsc_induction_protocol",
    "organoid",
    "process",
    "project",
    "protocol",
    "sequencing_protocol",
    "specimen_from_organism"
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
    * extract the necessary info from a metadata file and put it into a form that makes it easy to
    * pass in to the table format
    *
    * @param entityType the metadata entity type to prepend for the id field
    * @param fileName the raw filename of the metadata file
    * @param metadata the content of the metadata file in Msg format
    * @return a Msg object in the desired output format
    */
  def transformMetadata(entityType: String, fileName: String, metadata: Msg): Msg = {
    // extract info from file name
    val matches = metadataPattern
      .findFirstMatchIn(fileName)
      .getOrElse(
        throw new Exception(
          s"transformMetadata: error when finding id and version from file named $fileName"
        )
      )
    val entityId = matches.group(1)
    val entityVersion = matches.group(2)
    // and put in form we want
    Obj(
      mutable.LinkedHashMap[Msg, Msg](
        Str(s"${entityType}_id") -> Str(entityId),
        Str("version") -> Str(entityVersion),
        Str("content") -> Str(encode(metadata).getOrElse(""))
      )
    )
  }

  def transformFileMetadata(entityType: String, fileName: String, metadata: Msg): Msg = {
    val basicMetadata = transformMetadata(entityType, fileName, metadata)

    // get the name of the data file and the MD5 checksum
    val coreFileMetadata = metadata.read[Msg]("file_core")
    val checksum = coreFileMetadata.tryRead[String]("checksum")
    val dataFileName = coreFileMetadata.read[String]("file_name")
    // parse the data file name
    val matches = fileDataPattern
      .findFirstMatchIn(dataFileName)
      .getOrElse(
        throw new Exception(
          s"transformMetadata: error when finding file id and version from file named $dataFileName"
        )
      )
    val fileId = matches.group(2)
    val fileVersion = matches.group(3)
    // and put in form we want
    // TODO prepend basicMetadata value
    Obj(
      mutable.LinkedHashMap[Msg, Msg](
        Str("file_id") -> Str(fileId),
        Str("file_version") -> Str(fileVersion),
        Str("content_hash") -> Str(checksum.getOrElse(""))
      )
    )
  }

  def encode(msg: Msg): Option[String] =
    if (msg.obj.isEmpty) {
      None
    } else {
      Some(upack.transform(msg, StringRenderer()).toString)
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
    entityType: String
  ): ClosedTap[String] = {
    // get the readable files for the given input path
    val readableFiles = getReadableFiles(
      s"$inputPrefix/metadata/${entityType}/**.json",
      context
    )
    // then convert json to msg and get the filename
    val processedData = jsonToFilenameAndMsg(entityType)(readableFiles)
      .withName(s"Pre-process ${entityType} metadata")
      .map {
        case (filename, metadata) => transformMetadata(entityType, filename, metadata)
      }
    // then write to storage
    StorageIO.writeJsonLists(
      processedData,
      entityType,
      s"${outputPrefix}/metadata/${entityType}"
    )
  }
}

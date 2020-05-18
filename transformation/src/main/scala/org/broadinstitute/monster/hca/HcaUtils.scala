package org.broadinstitute.monster.hca

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.{FileIO, ReadableFileCoder}
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment
import org.broadinstitute.monster.common.StorageIO
import org.broadinstitute.monster.common.msg.{JsonParser, UpackMsgCoder}
import upack.{Msg, Obj, Str}

import scala.collection.mutable
import scala.util.matching.Regex

object HcaUtils {
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)
  implicit val readableFileCoder: Coder[ReadableFile] = Coder.beam(new ReadableFileCoder)

  // format is: metadata/{entity_type}/{entity_id}_{version}.json,
  // but filename returns just {entity_id}_{version}.json, so that is what we deal with.
  val metadataPattern: Regex = "([\\S]+)_([\\S]+).json".r

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
  def jsonToFilenameAndMsg(tableName: String): SCollection[ReadableFile] => SCollection[(String, Msg)] =
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
    val matches = HcaUtils.metadataPattern
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
        Str("content") -> metadata
      )
    )
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
      .withName(s"Convert ${entityType} from JSON to Msg in output form")
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

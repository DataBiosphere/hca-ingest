package org.broadinstitute.monster.hca

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.{FileIO, ReadableFileCoder}
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment
import org.broadinstitute.monster.common.msg.{JsonParser, UpackMsgCoder}
import upack.{Msg, Obj, Str}

import scala.collection.mutable
import scala.util.matching.Regex

object HcaUtils {
  // read data in from input bucket
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)
  implicit val readableFileCoder: Coder[ReadableFile] = Coder.beam(new ReadableFileCoder)

  val metadataPattern: Regex = "([\\S]+)_([\\S]+).json".r
  // if the filename is not just the thing after the last slash, then might need to prepend: metadata\/([\S]+)\/

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
  def jsonToMsg(tableName: String): SCollection[ReadableFile] => SCollection[(String, Msg)] =
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
    * @param fileName
    * @param metadata
    * @return
    */
  def transformMetadata(entityType: String, fileName: String, metadata: Msg): Msg = {
    // needs to extract info from file name
    // format is: metadata/{entity_type}/{entity_id}_{version}.json
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
    * Given a pattern matching JSONs, get the JSONs as ReadableFiles and convert each JSON to Msg and get is filepath.
    *
    * @param context context of the main pipeline
    * @param inputDir the root directory containing JSONs to be converted
    * @param relativeFilePath the file path containing JSONs to be converted relative to the input root directory
    */
  def processMetadata(
    context: ScioContext,
    inputDir: String,
    entityType: String,
    relativeFilePath: String = "**.json"
  ): SCollection[Msg] = {
    // get the readable files for the given input path
    val readableFiles = getReadableFiles(
      s"$inputDir/metadata/${entityType}/$relativeFilePath",
      context
    )

    // then convert json to msg and get the filename
    jsonToMsg(entityType)(readableFiles).map {
      case (filename, metadata) => transformMetadata(entityType, filename, metadata)
    }
  }
}

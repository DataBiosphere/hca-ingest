package org.broadinstitute.monster.hca

import java.nio.channels.Channels

import better.files._
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment
import org.apache.beam.sdk.io.{FileIO, ReadableFileCoder}
import org.broadinstitute.monster.common.msg.{JsonParser, UpackMsgCoder}
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.hca.jadeschema.table.AnalysisProcess
import upack._

import scala.util.matching.Regex

object HcaPipelineBuilder extends PipelineBuilder[Args] {

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    // read data in from input bucket
    implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)
    implicit val readableFileCoder: Coder[ReadableFile] = Coder.beam(new ReadableFileCoder)

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
        _.flatMap { file =>
          Channels
            .newInputStream(file.open())
            .autoClosed
            .apply { stream =>
              List(
                (
                  file.getMetadata.resourceId.getFilename,
                  JsonParser.parseEncodedJson(stream.toString)
                )
              )
            }
        }
      }

    def transformMetadata(fileName: String, metadata: Msg): Msg = {

      // needs to extract info from file name
      // format is: metadata/{entity_type}/{entity_id}_{version}.json
      // FIXME probably want this outside the method so we don't recreate it every time?
      val metadataPattern: Regex = "metadata\\/([\\S]+)\\/([\\S]+)_([\\S]+).json".r
      val matches = metadataPattern
        .findFirstMatchIn(fileName)
        .getOrElse(
          throw new Exception(
            s"transformMetadata: error when finding id and version from file named $fileName, using $metadataPattern as a pattern"
          )
        )
      val entityType = matches.group(1)
      val entityId = matches.group(2)
      val entityVersion = matches.group(3)
      // extract file info from Msg object if file entity type
      val schemaType = metadata.obj
        .getOrElse(
          Str("schema_type"),
          throw new IllegalStateException(s"Field schema_type not found in file $fileName")
        )
        .toString

      if (schemaType == "file") {
        val fileCore = metadata.obj
          .getOrElse(
            Str("file_core"),
            throw new IllegalStateException(s"Found a file with no file_core in $fileName")
          )
        val srcFileName = fileCore.obj
          .getOrElse(
            Str("file_name"),
            throw new IllegalStateException(s"Found a file with no file_name in $fileName")
          )
          .toString
        val srcFileFormat = fileCore.obj
          .getOrElse(
            Str("schema_version"),
            throw new IllegalStateException(s"Found a file with no schema_version in $fileName")
          )
          .toString
      }
      // and put in form we want
      ???
    }

    /**
      * Given a pattern matching JSONs, get the JSONs as ReadableFiles and convert each JSON to Msg and get is filepath.
      *
      * @param context context of the main pipeline
      * @param inputDir the root directory containing JSONs to be converted
      * @param relativeFilePath the file path containing JSONs to be converted relative to the input root directory
      */
    def extractAndConvert(
      context: ScioContext,
      inputDir: String,
      entityType: String,
      relativeFilePath: String = "**.json"
    ): SCollection[Msg] = {
      // get the readable files for the given input path
      val readableFiles = getReadableFiles(
        s"$inputDir/${entityType}/$relativeFilePath",
        context
      )

      // then convert json to msg and get the filename
      jsonToMsg(entityType)(readableFiles).map {
        case (filename, msg) =>
          // extract info from filename
          transformMetadata(filename, msg)
      }
    }
    // perform necessary shtuff to it

    // Write everything to storage

    // do this once for each metadata table
    StorageIO.writeJsonLists(
      ???,
      "Entity type",
      s"${args.outputPrefix}/entity_type"
    )
    ()
  }
}

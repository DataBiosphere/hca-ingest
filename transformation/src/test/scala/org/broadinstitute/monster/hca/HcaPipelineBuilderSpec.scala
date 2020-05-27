package org.broadinstitute.monster.hca

import org.broadinstitute.monster.common.msg.JsonParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HcaPipelineBuilderSpec extends AnyFlatSpec with Matchers {
  behavior of "HcaPipelineBuilder"

  it should "transform basic metadata" in {
    val exampleFileContent = JsonParser.parseEncodedJson("""{"beep": "boop"}""")
    val actualOutput = HcaPipelineBuilder.transformMetadata(
      entityType = "entity_type",
      fileName = "entityId_version.json",
      metadata = exampleFileContent
    )
    val expectedOutput = JsonParser.parseEncodedJson(
      json = """
               | {
               |   "entity_type_id": "entityId",
               |   "version": "version",
               |   "content": "{\"beep\":\"boop\"}"
               | }
               |""".stripMargin
    )

    actualOutput shouldBe expectedOutput
  }

  it should "transform basic file metadata" in {
    val exampleMetadataContent = JsonParser.parseEncodedJson(
      json = """
               | {
               |    "file_core": {
               |        "file_name": "some-id_some-version.numbers123_12-34_metrics_are_fun.csv",
               |        "format": "csv"
               |    },
               |    "schema_type": "file"
               | }
               |""".stripMargin
    )
    // TODO does this fail when checksum is "null"?
    val actualOutput = HcaPipelineBuilder.transformFileMetadata(
      entityType = "some_file_entity_type",
      fileName = "entity-id_entity-version.json",
      metadata = exampleMetadataContent
    )
    val expectedOutput = JsonParser.parseEncodedJson(
      json =
        """
          | {
          |   "some_file_entity_type_id": "entity-id",
          |   "version": "entity-version",
          |   "content": "{\"file_core\":{\"file_name\":\"some-id_some-version.numbers123_12-34_metrics_are_fun.csv\",\"format\":\"csv\"},\"schema_type\":\"file\"}",
          |   "file_id": "some-id",
          |   "file_version": "some-version.numbers123",
          |   "checksum": ""
          | }
          |""".stripMargin
    )

    actualOutput shouldBe expectedOutput
  }

  it should "transform file metadata with a directory and checksum" in {
    val exampleMetadataContent = JsonParser.parseEncodedJson(
      json = """
               | {
               |    "file_core": {
               |        "file_name": "a-directory/sub_directory/file-id_file-version_filename.json",
               |        "format": "json",
               |        "checksum": "abcdefg"
               |    }
               | }
               |""".stripMargin
    )
    val actualOutput = HcaPipelineBuilder.transformFileMetadata(
      entityType = "some_type",
      fileName = "123_456.json",
      metadata = exampleMetadataContent
    )
    val expectedOutput = JsonParser.parseEncodedJson(
      json =
        """
          | {
          |   "some_type_id": "123",
          |   "version": "456",
          |   "content": "{\"file_core\":{\"file_name\":\"a-directory/sub_directory/file-id_file-version_filename.json\",\"format\":\"json\",\"checksum\":\"abcdefg\"}}",
          |   "file_id": "file-id",
          |   "file_version": "file-version",
          |   "checksum": "abcdefg"
          | }
          |""".stripMargin
    )

    actualOutput shouldBe expectedOutput
  }
}

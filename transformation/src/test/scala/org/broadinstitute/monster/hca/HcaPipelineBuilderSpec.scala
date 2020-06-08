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

  it should "transform an empty metadata file" in {
    val actualOutput = HcaPipelineBuilder.transformMetadata(
      entityType = "entity_type",
      fileName = "id_version.json",
      metadata = JsonParser.parseEncodedJson("{}")
    )
    val expectedOutput = JsonParser.parseEncodedJson(
      json = """
               | {
               |   "entity_type_id": "id",
               |   "version": "version",
               |   "content": "{}"
               | }
               |""".stripMargin
    )

    actualOutput shouldBe expectedOutput
  }

  it should "transform file metadata with no directory in the filename" in {
    val exampleMetadataContent = JsonParser.parseEncodedJson(
      json = """
               | {
               |    "file_core": {
               |        "file_name": "some-id_some-version.numbers123_12-34_metrics_are_fun.csv",
               |        "format": "csv",
               |        "file_provenance": { "crc32c": "54321zyx" }
               |    },
               |    "schema_type": "file"
               | }
               |""".stripMargin
    )

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
          |   "content": "{\"file_core\":{\"file_name\":\"some-id_some-version.numbers123_12-34_metrics_are_fun.csv\",\"format\":\"csv\",\"file_provenance\":{\"crc32c\":\"54321zyx\"}},\"schema_type\":\"file\"}",
          |   "crc32c": "54321zyx",
          |   "source_file_id": "some-id",
          |   "source_file_version": "some-version.numbers123",
          |   "data_file_name": "some-id_some-version.numbers123_12-34_metrics_are_fun.csv"
          | }
          |""".stripMargin
    )

    actualOutput shouldBe expectedOutput
  }

  it should "transform file metadata with a directory in the filename" in {
    val exampleMetadataContent = JsonParser.parseEncodedJson(
      json = """
               | {
               |    "file_core": {
               |        "file_name": "a-directory/sub_directory/file-id_file-version_filename.json",
               |        "format": "json",
               |        "file_provenance": { "crc32c": "abcd1234" }
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
          |   "content": "{\"file_core\":{\"file_name\":\"a-directory/sub_directory/file-id_file-version_filename.json\",\"format\":\"json\",\"file_provenance\":{\"crc32c\":\"abcd1234\"}}}",
          |   "crc32c": "abcd1234",
          |   "source_file_id": "file-id",
          |   "source_file_version": "file-version",
          |   "data_file_name": "a-directory/sub_directory/file-id_file-version_filename.json"
          | }
          |""".stripMargin
    )

    actualOutput shouldBe expectedOutput
  }

  it should "correctly generate file ingest requests" in {
    val exampleProcessedFileMetadata = JsonParser.parseEncodedJson(
      json =
        """
          | {
          |   "some_type_id": "123",
          |   "version": "456",
          |   "content": "{\"file_core\":{\"file_name\":\"a-directory/sub_directory/file-id_file-version_filename.json\",\"format\":\"json\",\"file_crc32c\":\"abcd1234\"}}",
          |   "crc32c": "abcd1234",
          |   "source_file_id": "file-id",
          |   "source_file_version": "file-version",
          |   "data_file_name": "a-directory/sub_directory/file-id_file-version_filename.json"
          | }
          |""".stripMargin
    )
    val actualOutput = HcaPipelineBuilder.generateFileIngestRequest(
      metadata = exampleProcessedFileMetadata,
      inputPrefix = "some/local/directory"
    )
    val expectedOutput = JsonParser.parseEncodedJson(
      json =
        """
          | {
          |   "source_path": "some/local/directory/data/a-directory/sub_directory/file-id_file-version_filename.json",
          |   "target_path": "/a-directory/sub_directory/file-id_file-version_filename.json",
          |   "crc32c": "abcd1234"
          | }
          |""".stripMargin
    )

    actualOutput shouldBe expectedOutput
  }
}

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
               |   "describedBy": "a url",
               |    "file_core": {
               |        "file_name": "some-id_some-version.numbers123_12-34_metrics_are_fun.csv",
               |        "format": "csv"
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
          |   "content": "{\"describedBy\":\"a url\",\"file_core\":{\"file_name\":\"some-id_some-version.numbers123_12-34_metrics_are_fun.csv\",\"format\":\"csv\"},\"schema_type\":\"file\"}",
          |   "file_id": "some-id",
          |   "file_version": "some-version.numbers123",
          |   "content_hash": ""
          | }
          |""".stripMargin
    )

    actualOutput shouldBe expectedOutput
  }

  it should "transform file metadata with a checksum" in {}
}

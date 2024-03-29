package org.broadinstitute.monster.hca

import com.spotify.scio.testing.PipelineSpec
import org.broadinstitute.monster.common.PipelineCoders
import org.broadinstitute.monster.common.msg.JsonParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HcaPipelineBuilderSpec extends AnyFlatSpec with Matchers with PipelineSpec with PipelineCoders {
  behavior of "HcaPipelineBuilder"

  it should "transform basic metadata" in {
    val exampleFileContent = JsonParser.parseEncodedJson("""{"beep": "boop"}""")
    val actualOutput = HcaPipelineBuilder
      .transformMetadata(
        entityType = "entity_type",
        fileName = "entityId_version.json",
        metadata = exampleFileContent,
        inputPrefix = "prefix"
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

    actualOutput shouldBe Some(expectedOutput)
  }

  it should "transform an empty metadata file" in {
    val actualOutput = HcaPipelineBuilder
      .transformMetadata(
        entityType = "entity_type",
        fileName = "id_version.json",
        metadata = JsonParser.parseEncodedJson("{}"),
        inputPrefix = "prefix"
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

    actualOutput shouldBe Some(expectedOutput)
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

    val exampleDescriptorContent = JsonParser.parseEncodedJson(
      json = """
               | {
               |    "file_name": "some-id_some-version.numbers123_12-34_metrics_are_fun.csv",
               |    "file_id": "my-file-id",
               |    "file_version": "my-file-version",
               |    "crc32c": "54321zyx",
               |    "schema_type": "file_descriptor"
               | }
               |""".stripMargin
    )

    val actualOutput = HcaPipelineBuilder
      .transformFileMetadata(
        entityType = "some_file_entity_type",
        fileName = "entity-id_entity-version.json",
        metadata = exampleMetadataContent,
        descriptor = exampleDescriptorContent,
        inputPrefix = "prefix"
      )
    val expectedOutput = JsonParser.parseEncodedJson(
      json =
        """
          | {
          |   "some_file_entity_type_id": "entity-id",
          |   "version": "entity-version",
          |   "content": "{\"file_core\":{\"file_name\":\"some-id_some-version.numbers123_12-34_metrics_are_fun.csv\",\"format\":\"csv\",\"file_provenance\":{\"crc32c\":\"54321zyx\"}},\"schema_type\":\"file\"}",
          |   "crc32c": "54321zyx",
          |   "descriptor": "{\"file_name\":\"some-id_some-version.numbers123_12-34_metrics_are_fun.csv\",\"file_id\":\"my-file-id\",\"file_version\":\"my-file-version\",\"crc32c\":\"54321zyx\",\"schema_type\":\"file_descriptor\"}"
          | }
          |""".stripMargin
    )

    actualOutput shouldBe Some(expectedOutput)
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
    val exampleDescriptorContent = JsonParser.parseEncodedJson(
      json = """
               | {
               |    "file_name": "a-directory/sub_directory/file-id_file-version_filename.json",
               |    "file_id": "my-file-id",
               |    "file_version": "my-file-version",
               |    "crc32c": "54321zyx",
               |    "schema_type": "file_descriptor"
               | }
               |""".stripMargin
    )

    val actualOutput = HcaPipelineBuilder
      .transformFileMetadata(
        entityType = "some_type",
        fileName = "123_456.json",
        metadata = exampleMetadataContent,
        descriptor = exampleDescriptorContent,
        inputPrefix = "prefix"
      )
    val expectedOutput = JsonParser.parseEncodedJson(
      json =
        """
          | {
          |   "some_type_id": "123",
          |   "version": "456",
          |   "content": "{\"file_core\":{\"file_name\":\"a-directory/sub_directory/file-id_file-version_filename.json\",\"format\":\"json\",\"file_provenance\":{\"crc32c\":\"abcd1234\"}}}",
          |   "crc32c": "54321zyx",
          |   "descriptor": "{\"file_name\":\"a-directory/sub_directory/file-id_file-version_filename.json\",\"file_id\":\"my-file-id\",\"file_version\":\"my-file-version\",\"crc32c\":\"54321zyx\",\"schema_type\":\"file_descriptor\"}"
          | }
          |""".stripMargin
    )

    actualOutput shouldBe Some(expectedOutput)
  }

  it should "transform links.json file" in {
    val exampleMetadataContent = JsonParser.parseEncodedJson(
      json = """
               | {
               |    "schema_type": "link_bundle",
               |    "schema_version": "1.1.3",
               |    "links": []
               | }
               |""".stripMargin
    )
    val actualOutput = HcaPipelineBuilder
      .transformLinksFileMetadata(
        fileName = "123_456_789.json",
        metadata = exampleMetadataContent,
        inputPrefix = "prefix"
      )
    val expectedOutput = JsonParser.parseEncodedJson(
      json =
        """
          | {
          |   "content": "{\"schema_type\":\"link_bundle\",\"schema_version\":\"1.1.3\",\"links\":[]}",
          |   "links_id": "123",
          |   "version": "456",
          |   "project_id": "789"
          | }
          |""".stripMargin
    )

    actualOutput shouldBe Some(expectedOutput)
  }

  it should "correctly generate file ingest requests" in {
    val exampleId = "my-file-id"
    val exampleDescriptor = JsonParser.parseEncodedJson(
      json = s"""
                | {
                |    "file_name": "a-directory/sub_directory/file-id_file-version_filename.json",
                |    "file_id": "$exampleId",
                |    "file_version": "my-file-version",
                |    "crc32c": "54321zyx",
                |    "schema_type": "file_descriptor"
                | }
                |""".stripMargin
    )
    val (actualId, actualOutput) = HcaPipelineBuilder
      .generateFileIngestRequest(
        descriptor = exampleDescriptor,
        inputPrefix = "some/local/directory"
      )
    val expectedOutput = JsonParser.parseEncodedJson(
      json =
        """
          | {
          |   "source_path": "some/local/directory/data/a-directory/sub_directory/file-id_file-version_filename.json",
          |   "target_path": "/v1/my-file-id/54321zyx/a-directory/sub_directory/file-id_file-version_filename.json"
          | }
          |""".stripMargin
    )

    actualId shouldBe exampleId
    actualOutput shouldBe expectedOutput
  }

  it should "not mutate the json when validating" in {
    val exampleFileContent = JsonParser.parseEncodedJson(
      """
        |{
        |    "organ": {
        |        "text": "bräîn",
        |        "ontology": "astrocyte"
        |    },
        |    "schema_type": "biomaterial",
        |    "biomaterial_core": {
        |        "ncbi_taxon_id": [
        |            960600000000
        |        ],
        |        "biomaterial_id": "Q4_DEMO-sample_SAMN02797092",
        |        "has_input_biomaterial": "Q4_DEMO-donor_MGH30",
        |        "biomaterial_name": "Q4_DEMO-Single cell mRNA-seq_MGH30_A01",
        |        "supplementary_files": [
        |            "Q4_DEMO-protocol"
        |        ]
        |    },
        |    "organ_part": {
        |        "text": "glioblastoma"
        |    },
        |    "genus_species": [
        |        {
        |            "text": "Homo sapiens",
        |            "ontology": "NCBITaxon:9606"
        |        }
        |    ],
        |    "describedBy": "https://schema.humancellatlas.org/type/biomaterial/5.1.0/specimen_from_organism"
        |}
        |""".stripMargin
    )

    val exampleUrlAndFile = ("sampleFileName.json", exampleFileContent)
    runWithData(Seq(exampleUrlAndFile))(HcaPipelineBuilder.validateJson("prefix")) shouldBe
      Seq(exampleUrlAndFile)
  }
}

package org.broadinstitute.monster.hca

import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.broadinstitute.monster.common.PipelineCoders
import org.broadinstitute.monster.common.msg.JsonParser
import org.scalatest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class HcaPipelineValidationSpec
    extends AnyFlatSpec
    with Matchers
    with PipelineSpec
    with PipelineCoders {
  behavior of "HcaPipeline"

  // Success case
  it should "succeed with valid inputs" in {
    val opts = PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create()
    runWithRealContext(opts)(ctx =>
      HcaPipelineBuilder.buildPipeline(
        ctx,
        Args(
          "gs://broad-dsp-monster-hca-dev-test/inputs/Success",
          "gs://broad-dsp-monster-hca-dev-test/outputs-to-overwrite"
        )
      )
    ).waitUntilDone()
  }

  def pipelineTest(
    inputPath: String,
    expectedErrorMessage: String,
    count: Int = 1
  ): scalatest.Assertion = {
    val opts = PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create()
    val result = runWithRealContext(opts)(ctx =>
      HcaPipelineBuilder.buildPipeline(
        ctx,
        Args(
          s"gs://broad-dsp-monster-hca-dev-test/inputs/$inputPath",
          "gs://broad-dsp-monster-hca-dev-test/outputs-to-overwrite"
        )
      )
    ).waitUntilFinish()
    val expectedErrorMsg = JsonParser.parseEncodedJson(expectedErrorMessage)

    // make sure logs are correct
    val src = Source.fromFile("../logs/errors.log")
    val lines = src.getLines.toList
    src.close()
    lines.map(err => JsonParser.parseEncodedJson(err)).takeRight(count) should contain(
      expectedErrorMsg
    )
    // make sure counter was incremented right number of times
    result.counter(PostProcess.errorCount).attempted shouldBe count
    // this should fail things, so check that it fails with the specific exception
    assertThrows[HcaFailException](PostProcess.postProcess(result))
  }

  // FileMismatchError cases
  it should "fail with a FileMismatchError if the data file is missing" in {
    // we expect file 40d994d9-de67-458f-82f2-db971e082724.loom to be missing
    val expected =
      """
        |{"errorType":"FileMismatchError",
        |"filePath":"gs://broad-dsp-monster-hca-dev-test/inputs/FileMismatchErrorNoData/data/40d994d9-de67-458f-82f2-db971e082724.loom",
        |"fileName":"40d994d9-de67-458f-82f2-db971e082724.loom",
        |"message":"File has a descriptors/analysis_file and metadata/analysis_file but doesn't actually exist under data/."}
        |""".stripMargin
    pipelineTest("FileMismatchErrorNoData", expected)
  }

  it should "fail with a FileMismatchError if the metadata file is missing" in {
    // we expect file afile1_timestamp.json to be missing
    val expected =
      """
        |{"errorType":"FileMismatchError",
        |"filePath":"gs://broad-dsp-monster-hca-dev-test/inputs/FileMismatchErrorNoMetadata/metadata/analysis_file/afile1_timestamp.json",
        |"fileName":"afile1_timestamp.json",
        |"message":"File is present in descriptors/analysis_file but not in metadata/analysis_file."}
        |""".stripMargin
    pipelineTest("FileMismatchErrorNoMetadata", expected)
  }

  it should "fail with a FileMismatchError if the descriptor file is missing" in {
    // we expect file rfile1_timestamp.json to be missing
    val expected =
      """
        |{"errorType":"FileMismatchError",
        |"filePath":"gs://broad-dsp-monster-hca-dev-test/inputs/FileMismatchErrorNoDescriptors/descriptors/reference_file/rfile1_timestamp.json",
        |"fileName":"rfile1_timestamp.json",
        |"message":"File is present in metadata/reference_file but not in descriptors/reference_file."}
        |""".stripMargin
    pipelineTest("FileMismatchErrorNoDescriptors", expected)
  }

  // NoRegexPatternMatchError cases
  it should "fail with a NoRegexPatternMatchError if a links file name is malformed" in {
    // we expect file links1_timestamp_project_1 to be caught as invalid due to the underscore in the project name
    val expected =
      """
        |{"errorType":"NoRegexPatternMatchError",
        |"filePath":"gs://broad-dsp-monster-hca-dev-test/inputs/NoRegexPatternMatchErrorLinks/links/links1_timestamp_project_1.json",
        |"fileName":"links1_timestamp_project_1.json",
        |"message":"Error when finding links id, version, and project id from file."}
        |""".stripMargin
    pipelineTest("NoRegexPatternMatchErrorLinks", expected)
  }

  it should "fail with a NoRegexPatternMatchError if a metadata file name is malformed" in {
    // we expect file aprocess1timestamp.json to be caught as invalid since there are no underscores
    val expected =
      """
        |{"errorType":"NoRegexPatternMatchError",
        |"filePath":"gs://broad-dsp-monster-hca-dev-test/inputs/NoRegexPatternMatchErrorMetadata/metadata/analysis_process/aprocess1timestamp.json",
        |"fileName":"aprocess1timestamp.json",
        |"message":"Error when finding entity id and version from file name."}
        |""".stripMargin
    pipelineTest("NoRegexPatternMatchErrorMetadata", expected)
  }

  // MissingPropertyError cases
  it should "fail with a MissingPropertyError if a file doesn't have the describedBy property" in {
    // we expect file aprotocol1_timestamp.json to be missing the property of DescribedBy
    val expected =
      """
        |{"errorType":"MissingPropertyError",
        |"filePath":"gs://broad-dsp-monster-hca-dev-test/inputs/MissingPropertyErrorDescribedBy/metadata/analysis_protocol/aprotocol1_timestamp.json",
        |"fileName":"aprotocol1_timestamp.json",
        |"message":"File has no describedBy property."}
        |""".stripMargin
    pipelineTest("MissingPropertyErrorDescribedBy", expected)
  }

  it should "fail with a MissingPropertyError if a descriptor file doesn't have the filename property" in {
    // we expect file afile1_timestamp.json to be missing the property of file_name
    val expected =
      """
        |{"errorType":"MissingPropertyError",
        |"filePath":"gs://broad-dsp-monster-hca-dev-test/inputs/MissingPropertyErrorDescriptorsFilename/descriptors/analysis_file/afile1_timestamp.json",
        |"fileName":"afile1_timestamp.json",
        |"message":"Descriptor file has no file_name property."}
        |""".stripMargin
    pipelineTest("MissingPropertyErrorDescriptorsFilename", expected, 2)
  }

  it should "fail with a MissingPropertyError if a descriptor file doesn't have the crc32c property" in {
    // we expect file afile1_timestamp.json to be missing the property of crc32c
    val expected =
      """
        |{"errorType":"MissingPropertyError",
        |"filePath":"gs://broad-dsp-monster-hca-dev-test/inputs/MissingPropertyErrorDescriptorsChecksum/descriptors/analysis_file/afile1_timestamp.json",
        |"fileName":"afile1_timestamp.json",
        |"message":"Descriptor file has no crc32c property."}
        |""".stripMargin
    pipelineTest("MissingPropertyErrorDescriptorsChecksum", expected, 3)
  }

}

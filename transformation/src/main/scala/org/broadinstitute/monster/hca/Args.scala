package org.broadinstitute.monster.hca

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import org.broadinstitute.monster.HcaTransformationPipelineBuildInfo

@AppName("HCA transformation pipeline")
@AppVersion(HcaTransformationPipelineBuildInfo.version)
@ProgName("org.broadinstitute.monster.etl.hca.HCAPipeline")
case class Args(
  @HelpMessage("Path to the input bucket")
  inputPrefix: String,
  @HelpMessage("Path where transformed outputs should be written")
  outputPrefix: String
)

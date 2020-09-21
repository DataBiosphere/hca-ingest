package org.broadinstitute.monster.hca

import caseapp.core.help.Help
import caseapp.core.parser.Parser
import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

/** Entry-point for the HCA pipeline's Docker image. */
object HcaPipeline
    extends ScioApp[Args]()(Parser[Args], Help[Args], postProcess = PostProcess.postProcess) {
  override def pipelineBuilder: PipelineBuilder[Args] = HcaPipelineBuilder
}

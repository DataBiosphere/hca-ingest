package org.broadinstitute.monster.hca

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

/** Entry-point for the ClinVar pipeline's Docker image. */
object HcaPipeline extends ScioApp[Args] {
  override def pipelineBuilder: PipelineBuilder[Args] = HcaPipelineBuilder
}

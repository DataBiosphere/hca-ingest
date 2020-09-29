package org.broadinstitute.monster.hca

import com.spotify.scio.ScioResult
import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

/** Entry-point for the HCA pipeline's Docker image. */
object HcaPipeline extends ScioApp[Args]() {
  override def pipelineBuilder: PipelineBuilder[Args] = HcaPipelineBuilder
  override def postProcess: ScioResult => Unit = PostProcess.postProcess
}

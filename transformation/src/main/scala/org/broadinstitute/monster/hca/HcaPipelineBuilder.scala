package org.broadinstitute.monster.hca

import com.spotify.scio.ScioContext
import org.broadinstitute.monster.common.PipelineBuilder

object HcaPipelineBuilder extends PipelineBuilder[Args] {
  override def buildPipeline(ctx: ScioContext, args: Args): Unit = ???
}

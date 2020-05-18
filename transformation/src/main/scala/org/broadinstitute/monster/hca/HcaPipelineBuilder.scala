package org.broadinstitute.monster.hca

import com.spotify.scio.ScioContext
import org.broadinstitute.monster.common.PipelineBuilder

object HcaPipelineBuilder extends PipelineBuilder[Args] {

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    // process metadata for each non-file metadata entity
    Set(
      "aggregate_generation_protocol",
      "analysis_process",
      "analysis_protocol",
      "cell_line",
      "cell_suspension",
      "collection_protocol",
      "differentiation_protocol",
      "dissociation_protocol",
      "donor_organism",
      "enrichment_protocol",
      "image_specimen",
      "imaging_preperation_protocol",
      "imaging_protocol",
      "ipsc_induction_protocol",
      "organoid",
      "process",
      "project",
      "protocol",
      "sequencing_protocol",
      "specimen_from_organism"
    ).foreach { entityType =>
      HcaUtils.processMetadata(ctx, args.inputPrefix, args.outputPrefix, entityType)
    }
    ()
  }
}

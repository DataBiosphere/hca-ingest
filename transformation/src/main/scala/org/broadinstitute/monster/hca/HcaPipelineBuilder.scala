package org.broadinstitute.monster.hca

import com.spotify.scio.ScioContext
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}

object HcaPipelineBuilder extends PipelineBuilder[Args] {

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    // perform necessary shtuff to it
    val cell_lines = HcaUtils
      .processMetadata(ctx, args.inputPrefix, "cell_line")
//      .map(row => CellLine(row.read("id"), row.read("version"), row.read("content")))
    // Write everything to storage
    StorageIO.writeJsonLists(
      cell_lines,
      "Cell line",
      s"${args.outputPrefix}/metadata/cell_line"
    )
    ()
  }
}

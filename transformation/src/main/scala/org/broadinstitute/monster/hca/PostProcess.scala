package org.broadinstitute.monster.hca

import com.spotify.scio.{ScioMetrics, ScioResult}
import org.apache.beam.sdk.metrics.Counter

object PostProcess {
  val errorCount: Counter = ScioMetrics.counter("errorCount")

  def postProcess(result: ScioResult): Unit = {
    result.counter(errorCount).committed.fold(())(count => if (count > 0) throw new HcaFailException)
  }
}

class HcaFailException extends Exception

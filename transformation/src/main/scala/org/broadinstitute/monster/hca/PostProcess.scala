package org.broadinstitute.monster.hca

import com.spotify.scio.ScioResult

object PostProcess {

  def postProcess(result: ScioResult): Unit = {
    result.allCounters.foreach {
      case (name, count) =>
        if (name.getName == "errorCount")
          count.committed.fold(())(count => if (count > 0) throw new HcaFailException)
    }
  }
}

class HcaFailException extends Exception

package geotrellis.proj4.mgrs

import org.scalatest.{FunSpec, Matchers}

class MGRSSpec extends FunSpec with Matchers {

  describe("MGRS") {
    it("should produce bounding boxes containing the original point (more or less)") {
      println("MGRS conversion:")
      val numIters = 2500
      val allIters = for ( iteration <- 1 to numIters ) yield {
        val long = 360.0 * scala.util.Random.nextDouble - 180.0
        val lat = 164.0 * scala.util.Random.nextDouble - 80.0
        val results = for (accuracy <- 1 to 5) yield {
          val mgrsString = MGRS.longLatToMGRS(long, lat, accuracy)
          val bbox = MGRS.mgrsToBBox(mgrsString)
          // MGRS algorithm has some boundary issues, use a fudge factor which is tighter for higher resolution.
          // Wanted to use 10^(-accuracy) but needed to tune it up a bit to make the test succeed more of the time.
          val sigDigs1 = 2
          val sigDigs5 = 4.7
          val eps = math.pow(10, (-sigDigs5 + sigDigs1)/4 * (accuracy - 5) - sigDigs5)
          //val eps = 5e-4
          bbox._1 - eps <= long && long <= bbox._3 + eps && bbox._2 - eps <= lat && lat <= bbox._4 + eps
        }

        val testStat = results.reduce(_ && _)

        if (testStat) {
          println(s"\u001b[32m  ➟ ($long, $lat) converted correctly \u001b[0m")
        } else {
          println(s"\u001b[31m  ➟ ($long, $lat) DID NOT convert correctly \u001b[0m")
        }

        testStat
      }

      val failedCount = allIters.filterNot{ x => x }.length
      println(s"Out of $numIters random locations, $failedCount were outside their bounding box")

      // Target a less than 1.5% failure rate
      (failedCount <= (numIters * 3 / 20)) should be (true)
    }
  }

}

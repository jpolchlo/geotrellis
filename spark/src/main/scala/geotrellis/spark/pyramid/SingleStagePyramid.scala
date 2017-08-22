package geotrellis.spark.pyramid

import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.raster._
import geotrellis.raster.merge._
import geotrellis.raster.resample._
import geotrellis.raster.prototype._
import geotrellis.util._

import org.apache.spark.Partitioner
import org.apache.spark.rdd._

import scala.reflect.ClassTag

object SingleStagePyramid {

  //import geotrellis.spark.pyramid.Options

  def apply[
    K: SpatialComponent: ClassTag
  ](source: RDD[(K, Raster[Tile])],  
    sourceZoom: Int,
    layoutScheme: ZoomedLayoutScheme,
    resampleMethod: ResampleMethod = NearestNeighbor
  ): RDD[((Int, K), Raster[Tile])] = {
    val exemplar = source.first._2
    val (w, h) = exemplar.dimensions

    //val sourceLayout = source.metadata.getComponent[LayoutDefinition]
    //val LayoutLevel(nextZoom, nextLayout) = layoutScheme.zoomOut(LayoutLevel(zoom, sourceLayout))

    val subpyramidHeight = (math.log(math.min(w, h)) / math.log(2)).toInt
    val pyramidBase = source.map{ case (key, raster) => ((sourceZoom, key), raster) }

    // Start from RDD[(SpatialKey, Tile)]
    // Reduce each tile into a sub-pyramid
    // While the finest level produced is not the minimum zoom
    //   Draw the finest level from the pyramids, aggregate, and repyramid
    // Union each flattened cluster of levels, aggregateByKey

    def pyramid(input: ((Int, SpatialKey), Raster[Tile])): Seq[((Int, SpatialKey), Raster[Tile])] = {
      val ((zoom, key), raster) = input
      val newKey = SpatialKey(key._1 / 2, key._2 / 2)
      if (zoom == 0 || math.min(raster.rows, raster.cols) == 1) {
        Seq( ((zoom - 1, newKey), raster) )
      } else {
        val newRaster = raster.resample(w / 2, h / 2, resampleMethod)
        pyramid( ((zoom - 1, newKey), newRaster) ) :+ ((zoom - 1, newKey), raster)
      }
    }

    def fuseTiles(input: RDD[((Int, SpatialKey), Raster[Tile])]): RDD[((Int, SpatialKey), Raster[Tile])] =
      input.combineByKey(
        {base: Raster[Tile] => Raster(base.tile.prototype(w, h), base.extent)},
        { (base, toMerge) => base.merge(toMerge, resampleMethod) }, 
        { (r1, r2) => r1.merge(r2) }
      )

    def mapAccumL[A, B, C](init: A, seq: Seq[B])(f: (A, B) => (A, C)): (A, Seq[C]) = {
      val (res, seq1) = seq.foldLeft((init, Seq.empty[C])){ case ((st, accum), el) => {
        val (newSt, result) = f(st, el)
                               (newSt, result +: accum)
      }}
      (res, seq1.reverse)
    }

    fuseTiles(mapAccumL(source, 1 to phases){ (base, _) => {
      val subpyramids = base.map(pyramid(_))
      val tops = subpyramids.map(_.head)
      val newbase = fuseTiles(tops)
                             (newbase, subpyramids.flatMap{x => x})
    }}.snd.reduce(_.union(_))).union(pyramidBase)
  }

}



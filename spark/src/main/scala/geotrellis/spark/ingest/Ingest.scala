/*
 * Copyright (c) 2014 DigitalGlobe.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.ingest

import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.op.transform._

import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.vector._
import geotrellis.vector.reproject._
import geotrellis.proj4._

import monocle.SimpleLens

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast

import spire.syntax.cfor._

trait Sink[K] {
  def apply(layerMetaData: LayerMetaData, raster: RasterRDD[K]): Unit
}

trait IsProjectedExtent[T] extends SimpleLens[T, ProjectedExtent]
trait IsSpatialKey[K] extends SimpleLens[K, SpatialKey]

/** Represents the ingest process. 
  * An ingest process produces one layer from a set of input rasters.
  * 
  * The ingest process has the following steps:
  * 
  *  - Reproject tiles to the desired CRS:  (CRS, RDD[(Extent, CRS), Tile)]) -> RDD[(Extent, Tile)]
  *  - Determine the appropriate layer meta data for the layer. (CRS, LayoutScheme, RDD[(Extent, Tile)]) -> LayerMetaData)
  *  - Resample the rasters into the desired tile format. RDD[(Extent, Tile)] => RasterRDD[K]
  *  - Save the layer.
  */

object Ingest extends Logging {
  /** Turn a sink into a pyramiding sink */
  def pyramid(sink: Sink): Sink  = { rdd =>
    def _pyramid(rdd: RasterRDD[SpatialKey]): Unit = {
      val md = rdd.metaData
      logInfo(s"Pyramid: Sinking RDD for level: ${md.level.id}")
      sink(rdd)
      if (rdd.metaData.level.id > 1) _pyramid(Pyramid.zoomOut(rdd.pyramidUp))
    }

    _pyramid(rdd)
  }

  def apply(sc: SparkContext): Ingest = new Ingest(sc)
}

class Ingest[T: IngestKey, K: SpatialKeyView](sc: SparkContext, layoutScheme: LayoutScheme)(createKey: (T, SpatialKey) => K) {
  def apply(sourceTiles: RasterRDD[(T, Tile)], layerName: String, destCRS: CRS) = {
    val reprojected = 
      source
        .reproject(destCRS)

    val layerMetaData = reprojected.createMetaData(layerName, layoutScheme)

    val mosaic = 
      reprojected.mosaic(layerMetaData.rasterMetaData)(createKey)


  }
}

class Ingest(sc: SparkContext) {
  import Ingest.Sink

  def reproject(destCRS: CRS): RDD[((Extent, CRS), Tile)] => RDD[((Extent, CRS), Tile)] = {
    _.map { case ((extent, crs), tile) =>
      val (reprojectedTile, reprojectedExtent) = tile.reproject(extent, crs, destCRS)
      (reprojectedExtent, destCRS) -> reprojectedTile
    }
  }

  def setMetaData(zoomScheme: LayoutScheme): RDD[((Extent, CRS), Tile)] => RasterRDD[Extent] =
    { sourceTiles =>
      // TODO: This methods seems like it could be generic, factor it out when there is another use case
      // TODO: Allow variance of ZoomScheme and TileIndexScheme
      val tileIndexScheme: TileIndexScheme = RowIndexScheme

      val (uncappedExtent, cellType, cellSize, crs): (Extent, CellType, CellSize, CRS) =
        sourceTiles
          .map { case ((extent, crs), tile) =>
            (extent, tile.cellType, CellSize(extent, tile.cols, tile.rows), crs)
           }
          .reduce { (t1, t2) =>
            val (e1, ct1, cs1, crs1) = t1
            val (e2, ct2, cs2, crs2) = t2
            (
              e1.combine(e2),
              ct1.union(ct2),
              if(cs1.resolution < cs2.resolution) cs1 else cs2,
              crs1
            )
           }

      val worldExtent = crs.worldExtent
      val layerLevel: ZoomLevel = zoomScheme.layoutFor(worldExtent, cellSize)

      val extentIntersection = worldExtent.intersection(uncappedExtent).get

      val metaData = RasterMetaData(layerName, cellType, extentIntersection, crs, layerLevel, tileIndexScheme)

      new RasterRDD(sourceTiles.map{ case ((extent, _), tile) => extent -> tile}, metaData)
    }

  def mosaic(source: RasterRDD[Extent]): RasterRDD[SpatialKey] = {
    source.mosaic(e => e, (extent, tileId) => tileId)
  }

  def apply(source: =>RDD[((Extent, CRS), Tile)], sink:  Sink, destCRS: CRS, zoomScheme: ZoomScheme): Unit = {
    val reprojected = 
      source
        .reproject(destCRS)

    val layerMetaData = reprojected.createMetaData

    val mosaic = 
      reprojected.mosaic(layerMetaData.rasterMetaData) { (extent, spatialKey) => spatialKey }
      setMetaData(layerName, zoomScheme) |>
      mosaic |>
      sink
}

package geotrellis.spark.ingest

import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.hadoop.formats.NetCdfBand

import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.proj4.CRS

import org.apache.spark._
import org.apache.spark.rdd._

/**
 * Ingests raw multi-band NetCDF tiles into a re-projected and tiled RasterRDD
 */
object NetCDFIngest extends Logging {
  type Source = RDD[(NetCdfBand, Tile)]
  type Sink = RasterRDD[TimeSpatialKey] => Unit


  def apply (sc: SparkContext)(source: Source, sink:  Sink, destCRS: CRS, tilingScheme: TilingScheme = TilingScheme.TMS): Unit = {
    val reprojected = source.map {
      case (band, tile) =>
        val (reprojectedTile, reprojectedExtent) = tile.reproject(band.extent, band.crs, destCRS)
        band.copy(crs = destCRS, extent = reprojectedExtent) -> reprojectedTile
    }

    // First step is to build metaData
    val metaData: RasterMetaData = {
      // Note: dimensions would have changed from the stored if we actually reprojected
      val (band, dims, cellType) =
        reprojected.map {
          case (band, tile) => (band, tile.dimensions, tile.cellType)
        }.first // HERE we make an assumption that all bands share the same extent, therefor the first will do

      val layerLevel: ZoomLevel = tilingScheme.layoutFor(band.crs, CellSize(band.extent, dims))
      RasterMetaData(cellType, band.extent, band.crs, layerLevel, RowIndexScheme)
    }

    val raster =
      new RasterRDD(reprojected, metaData) // WARNING: I'm discarding band.varName here // TODO deal with this
        .mosaic(extentOf = _.extent, toKey = (band, tileId) => TimeSpatialKey(tileId, band.time))

    sink(raster)
  }
}

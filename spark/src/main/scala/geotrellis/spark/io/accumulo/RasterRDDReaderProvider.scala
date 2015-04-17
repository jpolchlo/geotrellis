package geotrellis.spark.io.accumulo

import geotrellis.spark._
import geotrellis.spark.utils._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.raster._

import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import org.apache.accumulo.core.data.{Key, Value, Range => ARange}

import scala.reflect.ClassTag

abstract class RasterRDDReaderProvider[K: ClassTag] {

  def setFilters(job: Job, layerId: LayerId, filterSet: FilterSet[K], keyBounds: KeyBounds[K], index: KeyIndex[K]): Unit

  def reader(instance: AccumuloInstance, metaData: AccumuloLayerMetaData, keyBounds: KeyBounds[K], index: KeyIndex[K])(implicit sc: SparkContext): FilterableRasterRDDReader[K] = {
    new FilterableRasterRDDReader[K] {
      def read(layerId: LayerId, filters: FilterSet[K]): RasterRDD[K] = {
        val AccumuloLayerMetaData(rasterMetaData, _, _, tileTable) = metaData
        val job = Job.getInstance(sc.hadoopConfiguration)
        instance.setAccumuloConfig(job)
        InputFormatBase.setInputTableName(job, tileTable)
        setFilters(job, layerId, filters, keyBounds, index)
        val rdd = sc.newAPIHadoopRDD(job.getConfiguration, classOf[BatchAccumuloInputFormat], classOf[Key], classOf[Value])
        val tileRdd =
          rdd.map { case (_, value) =>
            val (key, tileBytes) = KryoSerializer.deserialize[(K, Array[Byte])](value.get)
            val tile =
              ArrayTile.fromBytes(
                tileBytes,
                rasterMetaData.cellType,
                rasterMetaData.tileLayout.tileCols,
                rasterMetaData.tileLayout.tileRows
              )
            (key, tile: Tile)
          }

        new RasterRDD(tileRdd, rasterMetaData)
      }
    }
  }
}

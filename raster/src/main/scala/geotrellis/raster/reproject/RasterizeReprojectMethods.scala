/*
 * Copyright 2017 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.raster.reproject

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.util.MethodExtensions
import geotrellis.vector._

trait RasterRasterizeReprojectMethods[T <: CellGrid] extends MethodExtensions[Raster[T]] {

  def rasterizeReproject(
    srcCRS: CRS,
    destRegion: Polygon,
    destRE: RasterExtent, 
    destCRS: CRS,
    resampleMethod: ResampleMethod, 
    destCellType: CellType
  ): ProjectedRaster[T]

}

class ProjectedRasterRasterizeReprojectMethods[T <: CellGrid](val self: ProjectedRaster[T])(implicit ev: Raster[T] => RasterRasterizeReprojectMethods[T]) 
  extends MethodExtensions[ProjectedRaster[T]] {

  def rasterizeReproject(
    destRE: ProjectedRasterExtent, 
    resampleMethod: ResampleMethod, 
    destCellType: CellType
  ): ProjectedRaster[T] = {
    val destRegion = self.projectedExtent.reprojectAsPolygon(destRE.crs, 0.005)
    self.raster.rasterizeReproject(self.crs, destRegion, destRE, destRE.crs, resampleMethod, destCellType)
  }

}


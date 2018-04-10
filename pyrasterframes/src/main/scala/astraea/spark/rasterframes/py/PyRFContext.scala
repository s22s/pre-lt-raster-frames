/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package astraea.spark.rasterframes.py

import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import org.apache.spark.sql._
import astraea.spark.rasterframes._
import astraea.spark.rasterframes.util.withAlias
import com.vividsolutions.jts.geom.Geometry
import geotrellis.raster.{ArrayTile, CellType, Tile}
import geotrellis.spark.io._
import org.locationtech.geomesa.spark.jts.util.WKBUtils
import spray.json._


/**
 * py4j access wrapper to RasterFrame entry points.
 *
 * @author sfitch 
 * @since 11/6/17
 */
class PyRFContext(implicit sparkSession: SparkSession) extends RasterFunctions {
  sparkSession.withRasterFrames

  /**
    * Base conversion to RasterFrame
    */
  def asRF(df: DataFrame): RasterFrame = {
    df.asRF
  }

  /** DESERIALIZATION **/

  def generateTile(cellType: String, cols: Int, rows: Int, bytes: Array[Byte]): ArrayTile = {
    ArrayTile.fromBytes(bytes, CellType.fromName(cellType), cols, rows)
  }

  def generateGeometry(obj: Array[Byte]): Geometry = {
    WKBUtils.read(obj)
  }

  def readSingleband(path: String, cols: Int, rows: Int): RasterFrame = {
    val scene = GeoTiffReader.readSingleband(path)
    scene.projectedRaster.toRF(cols, rows)
  }

  def tileColumns(df: DataFrame): Array[Column] =
    df.asRF.tileColumns.toArray

  def spatialKeyColumn(df: DataFrame): Column =
    df.asRF.spatialKeyColumn

  def temporalKeyColumn(df: DataFrame): Column =
    df.asRF.temporalKeyColumn.orNull

  def tileToIntArray(col: Column): Column = tileToArray[Int](col)

  def tileToDoubleArray(col: Column): Column = tileToArray[Double](col)

  def tileLayerMetadata(df: DataFrame): String =
    // The `fold` is required because an `Either` is retured, depending on the key type.
    df.asRF.tileLayerMetadata.fold(_.toJson, _.toJson).prettyPrint

  def normalizedDifference(left: Column, right: Column): Column = {
    withAlias("norm_diff", left, right)(
      localDivide(localSubtract(left, right), localAdd(left, right))
    ).as[Tile]
  }

  def spatialJoin(df: DataFrame, right: DataFrame): RasterFrame = {
    df.asRF.spatialJoin(right.asRF)
  }


}

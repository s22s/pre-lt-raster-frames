/*
 * Copyright 2017 Astraea, Inc.
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

package astraea.spark.rasterframes

import geotrellis.util.MethodExtensions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{Metadata, MetadataBuilder}

import scala.util.Try

/**
 * Extension methods over [[DataFrame]].
 *
 * @author sfitch 
 * @since 7/18/17
 */
abstract class DataFrameMethods extends MethodExtensions[DataFrame]{

  /** Add the metadata for the column with the given name. */
  def addColumnMetadata(colName: String, metadataKey: String, metadata: Metadata): DataFrame = {
    val mergedMD = self.schema.find(_.name == colName).map(col ⇒ {
      new MetadataBuilder().withMetadata(col.metadata).putMetadata(metadataKey, metadata).build()
    }).getOrElse(metadata)

    // Wish spark provided a better way of doing this.
    val df: DataFrame = self
    import df.sparkSession.implicits._
    val cols = self.columns.map {
      case c if c == colName ⇒ col(c) as (c, mergedMD)
      case c ⇒ col(c)
    }
    self.select(cols: _*)
  }


  /** Converts this DataFrame to a RasterFrame after ensuring it has:
   *
   * <ol type="a">
   * <li>a space or space-time key column
   * <li>one or more tile columns
   * <li>tile layout metadata
   * <ol>
   *
   * If any of the above are violated, and [[IllegalArgumentException]] is thrown.
   *
   * @return validated RasterFrame
   * @throws IllegalArgumentException when constraints are not met.
   */
  @throws[IllegalArgumentException]
  def asRF: RasterFrame = {
    val potentialRF = certifyRasterframe(self)

    require(potentialRF.findSpatialKeyField.nonEmpty, "A RasterFrame requires a column identified as a spatial key")

    require(potentialRF.tileColumns.nonEmpty, "A RasterFrame requires at least one tile colulmn")

    require(Try(potentialRF.tileLayerMetadata).isSuccess, "A RasterFrame requires embedded TileLayerMetadata")

    potentialRF
  }

  /**
   * Converts [[DataFrame]] to a RasterFrame if the following constraints are fulfilled:
   *
   * <ol type="a">
   * <li>a space or space-time key column
   * <li>one or more tile columns
   * <li>tile layout metadata
   * <ol>
   *
   * @return Some[RasterFrame] if constraints fulfilled, [[None]] otherwise.
   */
  def asRFSafely: Option[RasterFrame] = Try(self.asRF).toOption

  /**
   * Tests for the following conditions on the [[DataFrame]]:
   *
   * <ol type="a">
   * <li>a space or space-time key column
   * <li>one or more tile columns
   * <li>tile layout metadata
   * <ol>
   *
   * @return true if all constraints are fulfilled, false otherwise.
   */
  def isRF: Boolean = Try(self.asRF).isSuccess

  /** Internal method for slapping the RasterFreame seal of approval on a DataFrame.
   * Only call if if you are sure it has a spatial key and tile columns and TileLayerMetadata. */
  private[astraea] def certify = certifyRasterframe(self)
}

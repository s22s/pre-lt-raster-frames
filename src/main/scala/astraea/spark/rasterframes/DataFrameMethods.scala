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
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructType}
import org.apache.spark.sql.gt._

import scala.util.Try

/**
 * Extension methods over [[DataFrame]].
 *
 * @author sfitch
 * @since 7/18/17
 */
trait DataFrameMethods extends MethodExtensions[DataFrame] {

  /** Add the metadata for the column with the given name. */
  def addColumnMetadata(column: Column, metadataKey: String, metadata: Metadata): DataFrame = {

//    val query = self.queryExecution.analyzed.output
    val targetCol = self.select(column).schema.headOption
    require(targetCol.nonEmpty, "Couldn't find specified column in dataframe.")

    // Wish spark provided a better way of doing this.
    val cols = self.schema.fields.map { field ⇒
      if(field == targetCol.get) {
        val md = new MetadataBuilder().withMetadata(field.metadata).putMetadata(metadataKey, metadata).build()
        self(field.name) as (field.name, md)
      }
      else self(field.name)
    }
    self.select(cols: _*)
  }

  /** Get the metadata attached to the given column with the given key */
  def getColumnMetadata(column: Column, metadataKey: String): Option[Metadata] = {
    self.select(column).schema.fields.headOption.map(_.metadata.getMetadata(metadataKey))
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

    require(
      potentialRF.findSpatialKeyField.nonEmpty,
      "A RasterFrame requires a column identified as a spatial key"
    )

    require(potentialRF.tileColumns.nonEmpty, "A RasterFrame requires at least one tile colulmn")

    require(
      Try(potentialRF.tileLayerMetadata).isSuccess,
      "A RasterFrame requires embedded TileLayerMetadata"
    )

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

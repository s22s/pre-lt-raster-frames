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

package org.apache.spark.sql.gt.functions

import geotrellis.raster.Tile
import geotrellis.raster.histogram.{Histogram, StreamingHistogram}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.gt.types.{HistogramUDT, TileUDT}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Statistics aggregation function for tiles.
 *
 * @author sfitch 
 * @since 4/24/17
 */
class AggregateHistogramFunction extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("value", TileUDT) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("buffer", HistogramUDT) :: Nil)

  override def dataType: DataType = new HistogramUDT()

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer(0) = StreamingHistogram()

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val hist = buffer.getAs[Histogram[Double]](0)
    val tile = input.getAs[Tile](0)
    buffer(0) = hist.merge(StreamingHistogram.fromTile(tile))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val hist1 = buffer1.getAs[Histogram[Double]](0)
    val hist2 = buffer2.getAs[Histogram[Double]](0)
    buffer1(0) = hist1 merge hist2
  }

  override def evaluate(buffer: Row): Histogram[Double] = {
    val result = buffer.getAs[Histogram[Double]](0)
    result
  }
}

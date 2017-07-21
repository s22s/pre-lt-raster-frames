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

import geotrellis.raster.histogram.Histogram
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Row, gt}
import org.apache.spark.sql.gt.Implicits._

/**
 * Statistics aggregation function for a full column of tiles.
 * @author sfitch
 * @since 5/18/17
 */
class AggregateStatsFunction extends AggregateHistogramFunction {
  override def dataType: DataType = histogramStatsEncoder.schema

  override def evaluate(buffer: Row): Any =
    buffer.getAs[Histogram[Double]](0).statistics().orNull
}

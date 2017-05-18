package org.apache.spark.sql.gt.functions

import geotrellis.raster.histogram.Histogram
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Row, gt}

/**
 * Statistics aggregation function for a full column of tiles.
 * @author sfitch
 * @since 5/18/17
 */
class AggregateStatsFunction extends AggregateHistogramFunction {
  override def dataType: DataType = gt.histogramStatsEncoder.schema

  override def evaluate(buffer: Row): Any =
    buffer.getAs[Histogram[Double]](0).statistics().orNull
}

package astraea.spark.rasterframes.functions

import org.apache.spark.sql.Row
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types.DataType

/**
 * Aggregation function that only returns the average. Depends on
 * [[StatsLocalTileAggregateFunction]] for computation and just
 * selects the mean result tile.
 *
 * @author sfitch
 * @since 8/11/17
 */
class LocalMeanAggregateFunction extends StatsLocalTileAggregateFunction {
  override def dataType: DataType = new TileUDT()
  override def evaluate(buffer: Row): Any = {
    val superRow = super.evaluate(buffer).asInstanceOf[Row]
    superRow.get(3)
  }
}

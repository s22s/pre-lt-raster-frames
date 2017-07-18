package astraea.spark.rasterframes

import geotrellis.util.MethodExtensions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.Metadata

/**
 * Extension methods over [[DataFrame]].
 * @author sfitch 
 * @since 7/18/17
 */
abstract class DataFrameMethods extends MethodExtensions[DataFrame]{

  /** Set the metadata for the column with the given name. */
  def setMetadata(colName: String, metadata: Metadata): DataFrame = {
    // Wish spark provided a better way of doing this.
    val cols = self.columns.map {
      case c if c == colName ⇒ col(c) as (c, metadata)
      case c ⇒ col(c)
    }
    self.select(cols: _*)
  }
}

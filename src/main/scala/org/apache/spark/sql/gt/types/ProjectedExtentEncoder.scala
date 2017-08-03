package org.apache.spark.sql.gt.types

import geotrellis.vector.ProjectedExtent
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.gt.Implicits

/**
 * Custom encoder for [[ProjectedExtent]]. Necessary because CRS isn't a case class.
 *
 * @author sfitch 
 * @since 8/2/17
 */
object ProjectedExtentEncoder extends DelegatingSubfieldEncoder {
  def apply(): ExpressionEncoder[ProjectedExtent] = {
    create(Seq(
      "extent" -> Implicits.extentEncoder,
      "crs" -> Implicits.crsEncoder
    ))
  }
}

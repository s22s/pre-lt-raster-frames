package org.apache.spark.sql.gt.types

import java.time.ZonedDateTime

import geotrellis.spark.TemporalProjectedExtent
import geotrellis.vector.ProjectedExtent
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.gt.Implicits

/**
 * Custom encoder for [[ProjectedExtent]]. Necessary because [[geotrellis.proj4.CRS]] within [[ProjectedExtent]] isn't a case class, and [[ZonedDateTime]] doesn't have a natural encoder.
 *
 * @author sfitch 
 * @since 8/2/17
 */
object TemporalProjectedExtentEncoder extends DelegatingSubfieldEncoder {
  def apply(): ExpressionEncoder[TemporalProjectedExtent] = {
    create(Seq(
      "extent" -> Implicits.extentEncoder,
      "crs" -> Implicits.crsEncoder,
      "instant" -> Encoders.scalaLong.asInstanceOf[ExpressionEncoder[Long]]
    ))
  }
}

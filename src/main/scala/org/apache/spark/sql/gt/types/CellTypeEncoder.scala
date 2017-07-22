package org.apache.spark.sql.gt.types

import geotrellis.raster.{CellType, DataType}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{ObjectType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.classTag

/**
 * Custom encoder for GT [[CellType]]. It's necessary since [[CellType]] is a type alias of
 * a type intersection.
 * @author sfitch 
 * @since 7/21/17
 */
object CellTypeEncoder {
  def apply(): Encoder[CellType] = {
    import org.apache.spark.sql.catalyst.expressions._
    import org.apache.spark.sql.catalyst.expressions.objects._
    val ctType = ScalaReflection.dataTypeFor[DataType]
    val schema = StructType(Seq(StructField("cellType", StringType, false)))
    val inputObject = BoundReference(0, ctType, nullable = false)

    val strType = ObjectType(classOf[String])
    val serializer: Expression =
      StaticInvoke(classOf[UTF8String], StringType, "fromString",
        Invoke(inputObject, "name", strType, Nil) :: Nil
      )

    val inputRow = GetColumnByOrdinal(0, schema)
    val deserializer: Expression =
      StaticInvoke(CellType.getClass, ctType, "fromName",
        Invoke(inputRow, "toString", strType, Nil) :: Nil
      )

    ExpressionEncoder[CellType](
      schema,
      flat = false,
      Seq(serializer),
      deserializer,
      classTag[CellType]
    )
  }
}

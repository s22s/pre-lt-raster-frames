package org.apache.spark.sql.gt.types
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, CreateStruct, Expression, GetStructField, If, IsNull, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.reflect.runtime.universe._

/**
 * Encoder builder for types composed of other fields with {{ExpressionEncoder}}s.
 *
 * @author sfitch 
 * @since 8/2/17
 */
trait DelegatingSubfieldEncoder {
  protected def create[T: TypeTag](fieldEncoders: Seq[(String, ExpressionEncoder[_])]): ExpressionEncoder[T] = {
    val schema = StructType(
      fieldEncoders.map{ case (name, encoder) ⇒
        StructField(name, encoder.schema, false)
      }
    )

    val parentType = ScalaReflection.dataTypeFor[T]

    val inputObject = BoundReference(0, parentType, nullable = false)
    val serializer = CreateNamedStruct(
      fieldEncoders.flatMap { case (name, encoder) ⇒
        val enc = encoder.serializer.map(_.transform {
          case r: BoundReference if r != inputObject ⇒
            Invoke(inputObject, name, r.dataType)
        })
        Literal(name) :: CreateStruct(enc) :: Nil
      }
    )

    val fieldDeserializers = fieldEncoders.map(_._2)
      .zipWithIndex.map { case (enc, index) =>
      val input = GetColumnByOrdinal(index, enc.schema)
      val deserialized = enc.deserializer.transformUp {
        case UnresolvedAttribute(nameParts) =>
          UnresolvedExtractValue(input, Literal(nameParts.head))
        case GetColumnByOrdinal(ordinal, _) => GetStructField(input, ordinal)
      }
      If(IsNull(input), Literal.create(null, deserialized.dataType), deserialized)
    }

    val deserializer: Expression = NewInstance(
      runtimeClass[T],
      fieldDeserializers,
      parentType,
      propagateNull = false
    )

    ExpressionEncoder(
      schema,
      flat = false,
      serializer.flatten,
      deserializer,
      typeToClassTag[T]
    )
  }
}

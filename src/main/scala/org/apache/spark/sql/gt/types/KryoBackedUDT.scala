package org.apache.spark.sql.gt.types

import geotrellis.spark.util.KryoSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

/**
 *
 * @author sfitch 
 * @since 4/18/17
 */
trait KryoBackedUDT[T >: Null] { self: UserDefinedType[T] ⇒

  implicit val targetClassTag: ClassTag[T]

  override val simpleString = typeName

  override def sqlType: DataType = StructType(Array(
    StructField(typeName + "_kryo", BinaryType)
  ))

  override def userClass: Class[T] = targetClassTag.runtimeClass.asInstanceOf[Class[T]]

  override def serialize(obj: T): Any = {
    Option(obj)
      .map(KryoSerializer.serialize(_)(targetClassTag))
      .map(InternalRow.apply(_))
      .orNull
  }

  override def deserialize(datum: Any): T = {
    Option(datum)
      .map(_.asInstanceOf[InternalRow])
      .flatMap(row ⇒ Option(row.getBinary(0)))
      .map(KryoSerializer.deserialize[T](_)(targetClassTag))
      .orNull
  }
}

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

package org.apache.spark.sql.gt.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenFallback, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, UnaryExpression}
import org.apache.spark.sql.types.{StructType, UserDefinedType}

/**
 * Catalyst expression for converting a UDT into a StructType.
 *
 * @author sfitch 
 * @since 4/12/17
 */
private[spark] case class UDTAsStructExpression[T >: Null](udt: UserDefinedType[T], child: Expression)
  extends UnaryExpression with CodegenFallback {

  require(udt.sqlType.isInstanceOf[StructType],
    "Only struct encoded UDTs supported right now. See `ExpressionEncoder` line 74 for possible workaround")

  override def prettyName: String = udt.typeName + "_asstruct"

  override def dataType: StructType = udt.sqlType.asInstanceOf[StructType]

  private lazy val projector = GenerateUnsafeProjection.generate(
    udt.sqlType.asInstanceOf[StructType].fields.zipWithIndex.map { case (field, index) ⇒
      BoundReference(index, field.dataType, true)
    }
  )

  override protected def nullSafeEval(input: Any): Any = {
    projector(input.asInstanceOf[InternalRow])
  }
}

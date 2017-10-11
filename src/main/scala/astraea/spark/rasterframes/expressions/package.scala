/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package astraea.spark.rasterframes

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types.{DataType, StringType}

/**
 * Module of Catalyst expressions for efficiently working with tiles.
 *
 * @author sfitch
 * @since 10/10/17
 */
package object expressions {
  case class CellType(child: Expression) extends UnaryExpression {

    def dataType: DataType = StringType

    override def checkInputDataTypes(): TypeCheckResult = {
      if(child.dataType.isInstanceOf[TileUDT]) TypeCheckSuccess
      else TypeCheckFailure(
        s"Expected '${TileUDT.typeName}' but received '${child.dataType.simpleString}'"
      )
    }

    override protected def nullSafeEval(input: Any): Any =
      input.asInstanceOf[InternalRow].getUTF8String(TileUDT.C.CELL_TYPE)

    protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
      defineCodeGen(ctx, ev, c ⇒ s"$c.getUTF8String(${TileUDT.C.CELL_TYPE})")
  }
}

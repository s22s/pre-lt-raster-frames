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

package astraea.spark.rasterframes.encoders

import astraea.spark.rasterframes.jts.SpatialEncoders
import geotrellis.vector.Point
import org.apache.spark.ml.attribute.UnresolvedAttribute
import org.apache.spark.sql.PointUDT
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.NewInstance
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Literal}

/**
 *
 * @author sfitch 
 * @since 12/17/17
 */
object PointEncoder extends SpatialEncoders {
  def apply(): ExpressionEncoder[Point] = {
    val userType = ScalaReflection.dataTypeFor[Point]
    val schema = jtsPointEncoder.schema
    val inputObject = BoundReference(0, userType, nullable = true)

    val serializer = jtsPointEncoder.serializer.map(_.transform {
      case r: BoundReference if r != inputObject ⇒
        InvokeSafely(inputObject, "jtsGeom", r.dataType)
    })

    val input = GetColumnByOrdinal(0, schema)

    jtsPointEncoder.deserializer

    val deserializer = NewInstance(runtimeClass[Point], Seq(jtsPointEncoder.deserializer), userType, propagateNull = true)

    ExpressionEncoder(schema, flat = false, serializer, deserializer, typeToClassTag[Point])
  }
}

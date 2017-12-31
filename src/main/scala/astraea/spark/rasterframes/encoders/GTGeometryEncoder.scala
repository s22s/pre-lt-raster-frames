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
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.objects.NewInstance
import geotrellis.vector.{Geometry ⇒ GTGeom}
import org.apache.spark.annotation.Experimental

import scala.reflect.runtime.universe._

/**
 * Experimental encoder for encoding Geotrellis vector types as JTS UDTs.
 *
 * @author sfitch 
 * @since 12/17/17
 */
object GTGeometryEncoder {
  import SpatialEncoders.jtsGeometryEncoder
  @Experimental
  @deprecated("probably going to go away", "always")
  def apply[G <: GTGeom: TypeTag](): ExpressionEncoder[G] = {
    val userType = ScalaReflection.dataTypeFor[G]
    val schema = jtsGeometryEncoder.schema
    val inputObject = BoundReference(0, userType, nullable = true)

    val serializer = jtsGeometryEncoder.serializer.map(_.transform {
      case r: BoundReference if r != inputObject ⇒
        InvokeSafely(inputObject, "jtsGeom", r.dataType)
    })

    val deserializer = NewInstance(runtimeClass[G], Seq(jtsGeometryEncoder.deserializer), userType, propagateNull = true)

    ExpressionEncoder(schema, flat = true, serializer, deserializer, typeToClassTag[G])
  }
}

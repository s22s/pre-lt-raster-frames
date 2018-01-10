/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package astraea.spark.rasterframes.jts

import com.vividsolutions.jts.geom.{Point ⇒ jtsPoint}
import geotrellis.util.MethodExtensions
import geotrellis.vector.{Extent, Point ⇒ gtPoint}
import org.apache.spark.sql.TypedColumn

/**
 *
 * @author sfitch 
 * @since 1/10/18
 */
trait Implicits {
  implicit class ExtentColumnWithIntersects(val self: TypedColumn[Any, Extent])
    extends MethodExtensions[TypedColumn[Any, Extent]] {

    def intersects(pt: jtsPoint): TypedColumn[Any, Boolean] =
      SpatialPredicates.intersects(self, SpatialConverters.geomlit(pt))

    def intersects(pt: gtPoint): TypedColumn[Any, Boolean] =
      SpatialPredicates.intersects(self, SpatialConverters.geomlit(pt.jtsGeom))
  }
}

object Implicits extends Implicits

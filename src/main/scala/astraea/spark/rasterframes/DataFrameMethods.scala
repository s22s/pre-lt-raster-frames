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

package astraea.spark.rasterframes

import geotrellis.util.MethodExtensions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.Metadata

/**
 * Extension methods over [[DataFrame]].
 * @author sfitch 
 * @since 7/18/17
 */
abstract class DataFrameMethods extends MethodExtensions[DataFrame]{

  /** Set the metadata for the column with the given name. */
  def setColumnMetadata(colName: String, metadata: Metadata): DataFrame = {
    // Wish spark provided a better way of doing this.
    val cols = self.columns.map {
      case c if c == colName ⇒ col(c) as (c, metadata)
      case c ⇒ col(c)
    }
    self.select(cols: _*)
  }
}

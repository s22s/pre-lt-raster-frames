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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Module support routines.
 *
 * @author sfitch 
 * @since 12/21/17
 */
package object jts {
  private def register(sqlContext: SQLContext, rule: Rule[LogicalPlan]): Unit = {
    if(!sqlContext.experimental.extraOptimizations.contains(rule))
      sqlContext.experimental.extraOptimizations :+= rule
  }

  def register(sqlContext: SQLContext): Unit = {
    register(sqlContext, SpatialRules)
  }
}

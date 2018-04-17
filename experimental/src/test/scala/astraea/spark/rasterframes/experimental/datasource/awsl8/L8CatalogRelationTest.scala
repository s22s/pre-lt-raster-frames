/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea. Inc.
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
 *
 */

package astraea.spark.rasterframes.experimental.datasource.awsl8

import astraea.spark.rasterframes.TestEnvironment
import org.apache.spark.sql.functions._

/**
 *
 * @author sfitch
 * @since 1/5/18
 */
class L8CatalogRelationTest  extends TestEnvironment {
  describe("Representing L8 scenes as a Spark data source") {
    it("should provide a non-empty catalog") {
      import spark.implicits._

      val catalog = spark.read.format(L8CatalogDataSource.NAME).load()

      val scenes = catalog.filter($"path" === 15 && $"row" === 34).orderBy(asc("cloudCover"))
      scenes.show(false)
      assert(scenes.count() > 1)
    }
  }
}

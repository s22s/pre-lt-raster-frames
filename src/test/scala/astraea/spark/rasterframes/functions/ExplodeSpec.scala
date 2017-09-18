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

package astraea.spark.rasterframes.functions

import astraea.spark.rasterframes._
import geotrellis.raster._

/**
 * Test rig for Tile operations associated with converting to/from
 * exploded/long form representations of the tile's data.
 *
 * @author sfitch 
 * @since 9/18/17
 */
class ExplodeSpec extends TestEnvironment with TestData {
  import sqlContext.implicits._

  describe("conversion to/from exploded representation of tiles") {

    it("should explode tiles") {
      val query = sql(
        """select st_explodeTiles(
          |  st_makeConstantTile(1, 10, 10, 'int8raw'),
          |  st_makeConstantTile(2, 10, 10, 'int8raw')
          |)
          |""".stripMargin)
      write(query)
      assert(query.select("cell_0", "cell_1").as[(Double, Double)].collect().forall(_ == ((1.0, 2.0))))
      val query2 = sql(
        """|select st_tileDimensions(tiles) as dims, st_explodeTiles(tiles) from (
           |select st_makeConstantTile(1, 10, 10, 'int8raw') as tiles)
           |""".stripMargin)
      write(query2)
      assert(query2.columns.length === 4)

      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")
      val exploded = df.select(explodeTiles($"tile1", $"tile2"))
      //exploded.printSchema()
      assert(exploded.columns.length === 4)
      assert(exploded.count() === 9)
      write(exploded)
    }


    it("should explode tiles with random sampling") {
      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")
      val exploded = df.select(explodeTileSample(0.5, $"tile1", $"tile2"))
      assert(exploded.columns.length === 4)
      assert(exploded.count() < 9)
    }

    it("should convert tile into array") {
      val query = sql(
        """select st_tileToArray(
          |  st_makeConstantTile(1, 10, 10, 'int8raw')
          |) as intArray
          |""".stripMargin)
      assert(query.as[Array[Int]].first.sum === 100)

      val tile = FloatConstantTile(1.1f, 10, 10, FloatCellType)
      val df = Seq[Tile](tile).toDF("tile")
      val arrayDF = df.select(tileToArrayDouble($"tile"))
      assert(arrayDF.first().sum === 110.0 +- 0.00001)
    }

  }
}

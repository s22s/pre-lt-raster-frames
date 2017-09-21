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

package examples

import astraea.spark.rasterframes._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql._

object Clustering extends App {

  // Utility for reading imagery from our test data set
  def readTiff(name: String): SinglebandGeoTiff =
    SinglebandGeoTiff(getClass.getResource(s"/$name").getPath)

  implicit val spark = SparkSession.builder().master("local[*]").appName(getClass.getName).getOrCreate()

  rfInit(spark.sqlContext)

  // The first step is to load multiple bands of imagery and construct
  // a single RasterFrame from them.
  val filenamePattern = "L8-B%d-Elkton-VA.tiff"
  val bandNumbers = 1 to 4

  // For each identified band, load the associated image file
  val joinedRF = bandNumbers
    .map { b ⇒ (b, filenamePattern.format(b)) }
    .map { case (b,f) ⇒ (b, readTiff(f)) }
    .map { case (b, t) ⇒ t.projectedRaster.toRF(s"band_$b") }
    .reduce(_ spatialJoin _)

  // We should see a single spatial_key column along with 4 columns of tiles.
  joinedRF.printSchema()

  // SparkML requires that each observation be in its own row, and those
  // observations be packed into a single `Vector`. The first step is to
  // "explode" the tiles into a single row per cell/pixel
  val exploded = joinedRF.select(
    joinedRF.spatialKeyColumn, explodeTiles(joinedRF.tileColumns: _*)
  )

  // As we see here, `explodeTiles` function adds `column_index` and `row_index` columns
  // reporting where the cell originated from in the source tile.
  exploded.show(8)

  // To "vectorize" the the band columns we use the SparkML `VectorAssembler`
  val assembler = new VectorAssembler()
    .setInputCols(joinedRF.tileColumns.map(_.toString).toArray)
    .setOutputCol("features")

  // Configure our clustering algorithm
  val kmeans = new KMeans().setK(3)

  // Combine the two stages
  val pipeline = new Pipeline().setStages(Array(assembler, kmeans))

  // Compute clusters
  val model = pipeline.fit(exploded)

  // Run the data through the model to assign cluster IDs to each
  val clustered = model.transform(exploded)
  clustered.show(8)

  // If we want to inspect the model statistics, the SparkML API requires us to go
  // through this unfortunate contortion:
  val clusterResults = model.stages.collect{ case km: KMeansModel ⇒ km}.head

  // Compute sum of squared distances of points to their nearest center
  val metric = clusterResults.computeCost(clustered)
  println("Within set sum of squared errors: " + metric)

  spark.stop()
}
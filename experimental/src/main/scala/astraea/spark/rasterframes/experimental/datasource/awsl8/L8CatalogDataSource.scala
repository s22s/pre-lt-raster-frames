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


import astraea.spark.rasterframes.util._
import java.net.URL
import java.nio.file.{Files, Path, Paths}
import java.time.{Duration, Instant}

import geotrellis.util.LazyLogging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

import scala.util.Try

/**
 *
 * @author sfitch
 * @since 9/28/17
 */
class L8CatalogDataSource extends DataSourceRegister with RelationProvider {
  val shortName = L8CatalogDataSource.NAME

  lazy val sceneListFile = L8CatalogDataSource.sceneListFile

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse("path", sceneListFile.toUri.toASCIIString)
    L8CatalogRelation(sqlContext, path)
  }
}

object L8CatalogDataSource extends LazyLogging {
  val NAME = "awsl8-catalog"
  val SCENE_LIST_NAME = "l8_scene_list.gz"
  val S3_SCENE_LIST = "http://landsat-pds.s3.amazonaws.com/c1/L8/scene_list.gz"
  lazy val MAX_LIST_AGE_HOURS: Int = sys.props.get("rasterframes.max.scene.list.age")
    .flatMap(v ⇒ Try(v.toInt).toOption)
    .getOrElse(48)

  private[this] def cacheDir: Path =
    sys.props.get("user.home")
      .map(Paths.get(_))
      .filter(root ⇒ Files.isDirectory(root) && Files.isWritable(root))
      .orElse(Option(Paths.get(".")))
      .map(_.resolve(".rasterFrames"))
      .map(base ⇒ base.when(Files.exists(_)).getOrElse(Files.createDirectory(base)))
      .getOrElse(Files.createTempDirectory("rf_"))

  private[awsl8] def sceneListFile: Path = {
    val dest = cacheDir.resolve(SCENE_LIST_NAME)
    dest.when(f ⇒ Files.exists(f) && !expired(f)).getOrElse {
      import sys.process._
      try {
        logger.debug(s"Downloading '$S3_SCENE_LIST' to '$dest'")
        (new URL(S3_SCENE_LIST) #> dest.toFile).!!
      }
      catch {
        case t: Throwable ⇒ Files.delete(dest)
          throw t
      }
      dest
    }
  }

  private def expired(p: Path): Boolean = {
    val time = Files.getLastModifiedTime(p)
    val exp = time.toInstant.isAfter(Instant.now().plus(Duration.ofHours(MAX_LIST_AGE_HOURS)))
    if(exp) logger.debug(s"'$p' is expired with mod time of '$time'")
    exp
  }
}



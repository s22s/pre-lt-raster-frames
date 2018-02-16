# Try this script in a `pyscript` REPL or submit with `spark-submit` as in the README
from pyspark.sql import SparkSession
# Get access to Raster Frames goodies. (this works fine with local master)
from pyrasterframes import *
from pyrasterframes.functions import *

# you can also tweak app name and master here, not necessary from pyspark REPL.
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
spark.withRasterFrames()

# Read a local file
rf = spark.rf.readGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff")
rf.createOrReplaceTempView("rf")
print("Tile columns: ", rf.tileColumns())
print("Spatial key column: ", rf.spatialKeyColumn())
print("Temporal key column: ", rf.temporalKeyColumn())

rf.select(
    rf.spatialKeyColumn(),
    tileDimensions("tile"),
    cellType("tile"),
    dataCells("tile"),
    noDataCells("tile"),
    tileMin("tile"),
    tileMean("tile"),
    tileMax("tile"),
    tileSum("tile"),
    renderAscii("tile"),
).show()

rf.agg(
    aggMean("tile"),
    aggDataCells("tile"),
    aggNoDataCells("tile"),
    aggStats("tile").alias("stat"),
    aggHistogram("tile")
).show()


# Demo of using local operations and SQL
spark.sql("SELECT tile, rf_makeConstantTile(1, 128,128, 'uint16') AS One, rf_makeConstantTile(2, 128,128, 'uint16') AS Two FROM rf") \
    .createOrReplaceTempView("r3")

ops = spark.sql("SELECT tile, rf_localAdd(tile, One) AS AndOne, "
                "rf_localSubtract(tile, One) AS LessOne, "
                "rf_localMultiply(tile, Two) AS TimesTwo, "
                "rf_localDivide(  tile, Two) AS OverTwo "
          "FROM r3")

ops.printSchema
ops.select(tileMean("tile"), tileMean("AndOne"), tileMean("LessOne"), tileMean("TimesTwo"), tileMean("OverTwo")).show()

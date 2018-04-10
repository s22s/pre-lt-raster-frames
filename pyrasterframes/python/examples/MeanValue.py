
#py_mv_imports
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
from pyspark.sql import *
#py_mv_imports

#py_mv_create_session
spark = SparkSession.builder. \
    master("local[*]"). \
    appName("RasterFrames"). \
    config("spark.ui.enabled", "false"). \
    getOrCreate(). \
    withRasterFrames()
#py_mv_create_session

#py_mv_create_rasterframe
rf = spark.read.geotiff("src/test/resources/L8-B8-Robinson-IL.tiff")
rf.show(5, False)
#py_mv_create_rasterframe

#py_mv_find_mean
tileCol = rf("tile")
rf.agg(aggNoDataCells(tileCol), aggDataCells(tileCol), aggMean(tileCol)).show(False)
#py_mv_find_mean
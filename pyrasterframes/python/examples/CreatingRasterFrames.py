#py_crf_imports
from pyrasterframes import *
from org.apache.spark.sql import *
#py_crf_imports

#py_crf_create_session
spark = SparkSession.builder. \
    master("local[*]"). \
    appName("RasterFrames"). \
    config("spark.ui.enabled", "false"). \
    getOrCreate(). \
    withRasterFrames()
#py_crf_create_session

#py_crf_more_imports
# No additional imports needed for Python.
#py_crf_more_imports

#py_crf_create_rasterframe
rf = spark.rasterframes.readGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff", 128, 128)
rf.show(5, False)
#py_crf_create_rasterframe

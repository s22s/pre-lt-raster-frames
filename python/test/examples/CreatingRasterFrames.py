#py_crf_imports
from pyrasterframes import *
#py_crf_imports

#py_crf_create_session
spark = SparkSession.builder
    .master("local[*]")
    .appName("RasterFrames")
    .getOrCreate()
    .withRasterFrames
#py_crf_create_session

#py_crf_create_rasterframe
rf = spark.rasterframes.readGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff", 128, 128)
rf.show(5, false)
#py_crf_create_rasterframe
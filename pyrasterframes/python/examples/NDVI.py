

#py_nvdi_imports
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
from pyspark.sql import *
from pyspark.sql.functions import udf
#py_nvdi_imports

#py_nvdi_create_session
spark = SparkSession.builder. \
    master("local[*]"). \
    appName("RasterFrames"). \
    config("spark.ui.enabled", "false"). \
    getOrCreate(). \
    withRasterFrames()
#py_nvdi_create_session

#py_ndvi_create_rasterframe
redBand = spark.read.geotiff("src/test/resources/L8-B4-Elkton-VA.tiff").withColumnRenamed('tile', 'red_band')
nirBand = spark.read.geotiff("src/test/resources/L8-B5-Elkton-VA.tiff").withColumnRenamed('tile', 'nir_band')
#py_ndvi_create_rasterframe

rf = redBand.spatialJoin(nirBand).withColumn("ndvi", normalizedDifference('red_band', 'nir_band')).asRF()

rf.printSchema()

spark.stop()
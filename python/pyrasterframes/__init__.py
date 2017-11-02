from __future__ import absolute_import
from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

__all__ = ['RFContext']

# Helpful info:
# http://aseigneurin.github.io/2016/09/01/spark-calling-scala-code-from-pyspark.html

class RFContext:
    def __init__(self, spark_session):
        self.spark_session = spark_session
        self.sc = spark_session.sparkContext
        self.gateway = self.sc._gateway
        self.jvm = self.gateway.jvm

        java_import(self.jvm, "astraea.spark.rasterframes.*")
        java_import(self.jvm, "geotrellis.raster.io.geotiff.SinglebandGeoTiff")

    def read_geotiff(self, path):
        return self.jvm.SinglebandGeoTiff(path)


def _rf_init(spark_session):
    return RFContext(spark_session)


# Patch new method on SparkSession to mirror Scala approach
SparkSession.withRasterFrames = _rf_init

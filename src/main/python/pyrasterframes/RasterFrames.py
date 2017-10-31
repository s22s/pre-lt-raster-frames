from py4j.java_gateway import java_import
from pyspark.sql import *


class RasterFrames:
    def __init__(self, sc):
        self.sc = sc
        self.jvm = sc._gateway.jvm

        java_import(self.jvm, "astraea.spark.rasterframes")

    def apply(self, params):
        self.jvm

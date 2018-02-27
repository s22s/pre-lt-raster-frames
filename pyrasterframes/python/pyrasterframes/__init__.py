from __future__ import absolute_import
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame, DataFrameReader
from pyspark.sql.types import *
import functools

# RasterFrame was moved to 'types.py' - do we want this here?
from pyrasterframes.types import *



__all__ = ['RFContext', 'types', 'functions']

class RFContext(object):
    """
    Entrypoint in to RasterFrames services
    """
    def __init__(self, spark_session):
        self._spark_session = spark_session
        self._gateway = spark_session.sparkContext._gateway
        self._jvm = self._gateway.jvm
        jsess = self._spark_session._jsparkSession
        self._jrfctx = self._jvm.astraea.spark.rasterframes.py.PyRFContext(jsess)

    def readGeoTiff(self, path, cols=128, rows=128):
        rf = self._jrfctx.readSingleband(path, cols, rows)
        return RasterFrame(rf, self._spark_session, self._jrfctx)


def _rf_init(spark_session):
    """Patches in RasterFrames functionality to PySpark session."""
    if not hasattr(spark_session, "rasterframes"):
        spark_session.rasterframes = RFContext(spark_session)
        #spark_session.sparkContext.rf = spark_session.rf
    return spark_session


def _reader(df_reader, format_key, path, **options):
    return df_reader.format(format_key).load(path, **options)


# Patch new method on SparkSession to mirror Scala approach
SparkSession.withRasterFrames = _rf_init

# Add DataSource methods to the DataFrameReader
DataFrameReader.geotiff = lambda df_reader, path: _reader(df_reader, "geotiff", path)
DataFrameReader.geotrellis = lambda df_reader, path: _reader(df_reader, "geotrellis", path)


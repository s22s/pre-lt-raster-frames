from __future__ import absolute_import
from pyspark.sql import SparkSession, DataFrame, DataFrameReader
from pyspark.sql.types import *
from astraea.spark.rasterframes.datasource.geotiff import *
from astraea.spark.rasterframes.datasource.geotrellis import *

# Import RasterFrame types and functions
from pyrasterframes.types import *
from pyrasterframes import rasterfunctions


def _rf_init(spark_session):
    """ Adds RasterFrames functionality to PySpark session."""
    if not hasattr(spark_session, "rasterframes"):
        spark_session.rasterframes = RFContext(spark_session)
    return spark_session


def _reader(df_reader, format_key, path, **options):
    """ Loads the file of the given type at the given path."""
    dFrame = df_reader.format(format_key).load(path, **options)
    return RasterFrame(dFrame._jdf, df_reader._spark.sparkSession)


# Patch new method on SparkSession to mirror Scala approach
SparkSession.withRasterFrames = _rf_init

# Add DataSource convenience methods to the DataFrameReader
# TODO: make sure this supports **options
DataFrameReader.geotiff = lambda df_reader, path: _reader(df_reader, "geotiff", path)
DataFrameReader.geotrellis = lambda df_reader, path: _reader(df_reader, "geotrellis", path)


from __future__ import absolute_import
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.column import Column, _to_java_column

__all__ = ['RFContext', 'RasterFrame', 'tileMean']

# Helpful info:
# http://aseigneurin.github.io/2016/09/01/spark-calling-scala-code-from-pyspark.html

class RFContext(object):
    def __init__(self, spark_session):
        self._spark_session = spark_session
        self._gateway = spark_session.sparkContext._gateway
        self._jvm = self._gateway.jvm
        jsess = self._spark_session._jsparkSession
        self._jrfctx = self._jvm.astraea.spark.rasterframes.py.PyRFContext(jsess)

    def readGeoTiff(self, path):
        rf = self._jrfctx.readSingleband(path)
        return RasterFrame(self._jrfctx, rf, self._spark_session)


class RasterFrame(DataFrame):
    def __init__(self, jrfctx, jdf, sql_ctx):
        DataFrame.__init__(self, jdf, sql_ctx)
        self._jrfctx = jrfctx

    def tileColumns(self):
        """
        Fetches columns of type Tile.
        :return: One or more Column instances associated with Tiles.
        """
        cols = self._jrfctx.tileColumns(self._jdf)
        return [Column(c) for c in cols]

    def spatialKeyColumn(self):
        """
        Fetch the tagged spatial key column.
        :return: Spatial key column
        """
        col = self._jrfctx.spatialKeyColumn(self._jdf)
        return Column(col)

    def temporalKeyColumn(self):
        """
        Fetch the temporal key column, if any.
        :return: Temporal key column, or None.
        """
        col = self._jrfctx.temporalKeyColumn(self._jdf)
        return col and Column(col)

# class TileUDT(UserDefinedType):
#     """
#     SQL user-defined type (UDT) for a GeoTrellis Tile.
#     """
#
#     @classmethod
#     def sqlType(cls):
#         return StructType([
#             StructField("cellType", StringType(), False),
#             StructField("cols", ShortType(), False),
#             StructField("rows", ShortType(), False),
#             StructField("data", BinaryType(), False)
#         ])
#
#     @classmethod
#     def module(cls):
#         return "pyrasterframes"
#
#     @classmethod
#     def scalaUDT(cls):
#         return "org.apache.spark.sql.gt.types.TileUDT"
#
#     def serialize(self, obj):
#         if isinstance(obj, Tile):
#             raise NotImplementedError("Not implemented yet")
#         else:
#             raise TypeError("cannot serialize %r of type %r" % (obj, type(obj)))
#
#     def deserialize(self, datum):
#         assert len(datum) == 4, \
#             "VectorUDT.deserialize given row with length %d but requires 4" % len(datum)
#         raise NotImplementedError("Not yet implemneted")
#
#     def simpleString(self):
#         return "rf_tile"
#
#
# class Tile(object):
#
#     __UDT__ = TileUDT()
#
#     def __init__(self, jTile):
#         self._tile = jTile



def _rf_init(spark_session):
    """Patches in RasterFrames functionality to PySpark session."""
    if not hasattr(spark_session, "rf"):
        spark_session.rf = RFContext(spark_session)
        spark_session.sparkContext.rf = spark_session.rf
    return spark_session

# Patch new method on SparkSession to mirror Scala approach
SparkSession.withRasterFrames = _rf_init

def tileMean(col):
    """
    Compute the Tile-wise mean
    """
    rfctx = SparkContext._active_spark_context.rf._jrfctx
    return Column(rfctx.tileMean(_to_java_column(col)))


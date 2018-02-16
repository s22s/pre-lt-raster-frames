from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import UserDefinedType
from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.types import *

__all__ = ['TileUDT', 'RasterFrame']

class RasterFrame(DataFrame):
    def __init__(self, jdf, spark_session, jrfctx):
        DataFrame.__init__(self, jdf, spark_session)
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

class TileUDT(UserDefinedType):
    """User-defined type (UDT).

    .. note:: WARN: Internal use only.
    """

    @classmethod
    def sqlType(self):
        return StructType([
            StructField("cellType", StringType(), False),
            StructField("cols", ShortType(), False),
            StructField("rows", ShortType(), False),
            StructField("data", BinaryType(), False)
        ])

    @classmethod
    def module(cls):
        return 'pyrasterframes'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.gt.types.TileUDT'

    def serialize(self, obj):
        raise TypeError("Not implemented yet")

    def deserialize(self, datum):
        raise TypeError("Not implemented yet")

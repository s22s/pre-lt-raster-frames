from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import UserDefinedType
from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.types import *


#  _________________________________________
# / This is Temporary! It must go away with \
# \ the release of pyrasterframes.          /
#  -----------------------------------------
#         \   ^__^
#          \  (@@)\_______
#             (__)\       )\/\
#                 ||----w |
#                 ||     ||

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

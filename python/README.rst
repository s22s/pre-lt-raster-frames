PyRasterFrames
--------------

To initialize PyRasterFrames:

    >>> from pyrasterframes import *
    >>> spark = SparkSession.builder \
    ...     .master("local[*]") \
    ...     .appName("Using RasterFrames") \
    ...     .config("spark.some.config.option", "some-value") \
    ...     .getOrCreate() \
    ...     .withRasterFrames


PyRasterFrames
--------------

PyRasterFrames provides a Python API for RasterFrames!

Getting started

Build everything.

    $ sbt pysparkCmd

This command also prints the necessary invocation of pyspark to get access to PyRasterFrames.

Get a PySpark REPL.

    $ pyspark --packages io.astraea.raster-frames:$VERSION --master local[2]

To initialize PyRasterFrames:

    >>> from pyrasterframes.functions import *

Now you have a `rf` method on your current Spark context as well as various SQL user-defined functions available. You can then try for example some of the commands in `test/testit.py`.

    >>> rf = spark.rf.readGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff")
    >>> print("Tile columns: ", rf.tileColumns())

To submit a script, just use the `--packages` flag with `spark-submit`.

    $ spark-submit --packages io.astraea:raster-frames:$VERSION --master local[2] python/test/testit.py


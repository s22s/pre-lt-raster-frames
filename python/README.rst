PyRasterFrames
--------------

PyRasterFrames provides a Python API for RasterFrames!

Getting started

Build the shaded JAR.

    $ sbt assembly

Install the python package (for development / local use)

    $ pip install -e python

Get a Spark REPL

    $ pyspark --jars target/scala-2.11/RasterFrames-assembly-$VERSION.jar --master spark://myhome.io:7077 --num-executors 4

You can then try for example some of the commands in `test/testit.py`.

Submit a script

    $ spark-submit --jars target/scala-2.11/RasterFrames-assembly-$VERSION.jar --master spark://foo.io:7077 \
        python/test/testit.py


To initialize PyRasterFrames:

    >>> from pyrasterframes import *
    >>> spark = SparkSession.builder \
    ...     .master("local[*]") \
    ...     .appName("Using RasterFrames") \
    ...     .config("spark.some.config.option", "some-value") \
    ...     .getOrCreate() \
    ...     .withRasterFrames


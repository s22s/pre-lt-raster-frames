from pyspark.sql import SparkSession

__all__ = ['RasterFrames']


def _rf_init(self):
    print(self)
    return self


# Patch new method on SparkSession to mirror Scala approach
SparkSession.withRasterFrames = _rf_init

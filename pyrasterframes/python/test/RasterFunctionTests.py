
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import *
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
import unittest

class RasterFunctionsTest(unittest.TestCase):

    def setup(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')
        self.spark.withRasterFrames()
        self.rf = self.spark.read.geotiff("resources/L8-B8-Robinson-IL.tiff")
        self.tileCol = 'tile'
        self.rf.show()

    def test_identify_columns(self):
        cols = self.rf.tileColumns()
        self.assertEqual(len(cols), 1, '`tileColumns` did not find the proper number of columns.')
        print("Tile columns: ", cols)
        col = self.rf.spatialKeyColumn()
        self.assertIsInstance(col, Column, '`spatialKeyColumn` was not found')
        print("Spatial key column: ", col)
        col = self.rf.temporalKeyColumn()
        self.assertIsNone(col, '`temporalKeyColumn` should be `None`')
        print("Temporal key column: ", col)

    def test_aggregations(self):
        aggs = self.rf.agg(
            aggMean(self.tileCol),
            aggDataCells(self.tileCol),
            aggNoDataCells(self.tileCol),
            aggStats(self.tileCol),
            aggHistogram(self.tileCol),
        )

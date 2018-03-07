
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import *
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
import os
import inspect
import pathlib
import unittest
import sys


def _floor_compare(val1, val2):
    return math.floor(val1) == math.floor(val2)

#############################
# THIS IS ALL TEMPORARY JUNK:
def _jar_path():
    jar_locs = [p for p in sys.path if p.endswith('.jar')]
    jar_path = os.path.abspath(jar_locs[0])
    return pathlib.Path(jar_path).resolve()



def _locate_dir():
    return os.path.abspath(inspect.getsourcefile(lambda:0))

_curr_dir = _locate_dir()

def we_are_frozen():
    # All of the modules are built-in to the interpreter, e.g., by py2exe
    return hasattr(sys, "frozen")

def module_path():
    if we_are_frozen():
        return os.path.dirname(sys.executable)
    return os.path.dirname(__file__)

##############################


class RasterFunctionsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.spark.withRasterFrames()
        filepath = pathlib.Path(_curr_dir).resolve().parent
        print(_curr_dir)
        print(filepath)
        #print(os.listdir(str(_jar_path()) + '/pyrasterframes/'))
        cls.rf = cls.spark.read.geotiff('L8-B8-Robinson-IL.tiff')
        cls.tileCol = 'tile'
        cls.rf.show()


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


    def test_general(self):
        df = self.rf.withColumn('dims',  tileDimensions(self.tileCol)) \
            .withColumn('type', cellType(self.tileCol)) \
            .withColumn('dCells', dataCells(self.tileCol)) \
            .withColumn('ndCells', noDataCells(self.tileCol)) \
            .withColumn('min', tileMin(self.tileCol)) \
            .withColumn('max', tileMax(self.tileCol)) \
            .withColumn('mean', tileMean(self.tileCol)) \
            .withColumn('sum', tileSum(self.tileCol)) \
            .withColumn('mean', renderAscii(self.tileCol))

        df.show()


    def test_aggregations(self):
        aggs = self.rf.agg(
            aggMean(self.tileCol),
            aggDataCells(self.tileCol),
            aggNoDataCells(self.tileCol),
            aggStats(self.tileCol),

            # Not currently working:
            # aggHistogram(self.tileCol),
        )
        row = aggs.first()
        self.assertTrue(_floor_compare(row['agg_mean(tile)'], 10160))
        self.assertTrue(row['agg_data_cells(tile)'] == self.rf.count())
        self.assertTrue(row['agg_nodata_cells(tile)'] == 0)
        self.assertTrue(row['aggStats(tile)'].dataCells == row['agg_data_cells(tile)'])
        aggs.show()


    def test_sql(self):
        self.rf.createOrReplaceTempView("rf")

        self.spark.sql("""SELECT tile, rf_makeConstantTile(1, 128,128, 'uint16') AS One, 
                            rf_makeConstantTile(2, 128,128, 'uint16') AS Two FROM rf""") \
            .createOrReplaceTempView("r3")

        ops = self.spark.sql("""SELECT tile, rf_localAdd(tile, One) AS AndOne, 
                                    rf_localSubtract(tile, One) AS LessOne, 
                                    rf_localMultiply(tile, Two) AS TimesTwo, 
                                    rf_localDivide(  tile, Two) AS OverTwo 
                                FROM r3""")

        ops.printSchema
        statsRow = ops.select(tileMean(self.tileCol).alias('base'),
                           tileMean("AndOne").alias('plus_one'),
                           tileMean("LessOne").alias('minus_one'),
                           tileMean("TimesTwo").alias('double'),
                           tileMean("OverTwo").alias('half')) \
                        .first()

        self.assertTrue(_floor_compare(statsRow.base, statsRow.plus_one - 1))
        self.assertTrue(_floor_compare(statsRow.base, statsRow.minus_one + 1))
        self.assertTrue(_floor_compare(statsRow.base, statsRow.double / 2))
        self.assertTrue(_floor_compare(statsRow.base, statsRow.half * 2))


def suite():
    functionTests = unittest.TestSuite()
    functionTests.addTest(RasterFunctionsTest('test_identify_columns'))
    functionTests.addTest(RasterFunctionsTest('test_general'))
    functionTests.addTest(RasterFunctionsTest('test_aggregations'))
    functionTests.addTest(RasterFunctionsTest('test_sql'))
    return functionTests


unittest.TextTestRunner().run(suite())




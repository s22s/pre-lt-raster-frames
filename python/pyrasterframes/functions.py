from __future__ import absolute_import
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.column import Column, _to_java_column


_rf_functions = {
    'explodeTiles': 'Create a row for each cell in Tile.',
    'tileDimensions': 'Query the number of (cols, rows) in a Tile.',
    'tileToArray': 'Flattens Tile into an array. A numeric type parameter is required',
    #'arrayToTile': 'Convert array in `arrayCol` into a Tile of dimensions `cols` and `rows',
    #'assembleTile': 'Create a Tile from  a column of cell data with location indexes'
    'cellType': 'Extract the Tile\'s cell type',
    #'withNoData': 'Assign a `NoData` value to the Tiles.',
    'aggHistogram': 'Compute the full column aggregate floating point histogram',
    'aggStats': 'Compute the full column aggregate floating point statistics',
    'aggMean': 'Computes the column aggregate mean',
    'aggDataCells': 'Computes the number of non-NoData cells in a column',
    'aggNoDataCells': 'Computes the number of NoData cells in a column',
    'tileMean': 'Compute the Tile-wise mean'
}


def _create_function(rfctx, name, doc=""):
    """ Create a mapping to Scala UDF by name"""
    def _(col):
        jcol = col._jc if isinstance(col, Column) else _to_java_column(col)
        jfcn = getattr(rfctx, name)
        return Column(jfcn(jcol))
    _.__name__ = name
    _.__doc__ = doc
    return _


def _register():
    from pyrasterframes import _rf_init
    spark = SparkSession._instantiatedContext
    _rf_init(spark)
    for name, doc in _rf_functions.items():
        globals()[name] = _create_function(spark.rf._jrfctx, name, doc)


_register()

__all__ = list(_rf_functions.keys())

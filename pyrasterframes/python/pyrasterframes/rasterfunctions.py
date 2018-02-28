from __future__ import absolute_import
from pyspark import SparkContext
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
    'tileMean': 'Compute the Tile-wise mean',
    'tileSum': 'Compute the Tile-wise sum',
    'tileMin': 'Compute the Tile-wise minimum',
    'tileMax': 'Compute the Tile-wise maximum',
    'localAdd': 'Add two Tiles',
    'localSubtract': 'Subtract two Tiles',
    'localMultiply': 'Multiply two Tiles',
    'localDivide': 'Divide two Tiles',
    'renderAscii': 'Render ASCII art of tile',
    'noDataCells': 'Count of NODATA cells',
    'dataCells': 'Count of cells with valid data',
}


__all__ = list(_rf_functions.keys())


def _create_function(name, doc=""):
    """ Create a mapping to Scala UDF by name"""
    def _(col):
        sc = SparkContext._active_spark_context
        jcol = _to_java_column(col)
        if not hasattr(sc, '_jrf_context'):
            raise AttributeError(
                "RasterFrames have not been enabled for the active session. Call 'SparkSession.withRasterFrames()'.")
        jfcn = getattr(sc._jrf_context, name)
        return Column(jfcn(jcol))
    _.__name__ = name
    _.__doc__ = doc
    _.__module__ = 'pyrasterframes'
    return _


def _register_functions():
    """ Register each function in the scope"""
    for name, doc in _rf_functions.items():
        globals()[name] = _create_function(name, doc)


_register_functions()
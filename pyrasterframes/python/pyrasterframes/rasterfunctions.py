from __future__ import absolute_import
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.column import Column, _to_java_column


def _checked_context():
    """ Get the active SparkContext and throw an error if it is not enabled for RasterFrames."""
    sc = SparkContext._active_spark_context
    if not hasattr(sc, '_rf_context'):
        print(vars(sc))
        raise AttributeError(
            "RasterFrames have not been enabled for the active session. Call 'SparkSession.withRasterFrames()'.")
    return sc._rf_context._jrfctx


def _create_withNoData():
    """ Create a function mapping to the Scala implementation."""
    def _(col, noDataVal):
        jfcn = getattr(_checked_context(), 'withNoData')
        return Column(jfcn(_to_java_column(col), noDataVal))
    _.__name__ = 'withNoData'
    _.__doc__ = "Assign a `NoData` value to the Tiles in the given Column."
    _.__module__ = 'pyrasterframes'
    return _


def _create_assembleTile():
    """ Create a function mapping to the Scala implementation."""
    def _(colIndex, rowIndex, cellData, numCols, numRows, cellType):
        jfcn = getattr(_checked_context(), 'assembleTile')
        return Column(jfcn(_to_java_column(colIndex), _to_java_column(rowIndex), _to_java_column(cellData), numCols, numRows, cellType))
    _.__name__ = 'assembleTile'
    _.__doc__ = "'Create a Tile from  a column of cell data with location indices"
    _.__module__ = 'pyrasterframes'
    return _


def _create_arrayToTile():
    """ Create a function mapping to the Scala implementation."""
    def _(arrayCol, numCols, numRows):
        jfcn = getattr(_checked_context(), 'arrayToTile')
        return Column(jfcn(_to_java_column(arrayCol), numCols, numRows))
    _.__name__ = 'arrayToTile'
    _.__doc__ = "'Convert array in `arrayCol` into a Tile of dimensions `numCols` and `numRows'"
    _.__module__ = 'pyrasterframes'
    return _


_rf_unique_functions = {
    'withNoData': _create_withNoData(),
    'assembleTile': _create_assembleTile(),
    'arrayToTile': _create_arrayToTile(),
}


_rf_column_functions = {
    'explodeTiles': 'Create a row for each cell in Tile.',
    'tileDimensions': 'Query the number of (cols, rows) in a Tile.',
    'box2D': 'Extracts the bounding box (envelope) of the geometry.',
    'tileToIntArray': 'Flattens Tile into an array of integers.',
    'tileToDoubleArray': 'Flattens Tile into an array of doubles.',
    'cellType': 'Extract the Tile\'s cell type',
    #'aggHistogram': 'Compute the full column aggregate floating point histogram',
    'aggStats': 'Compute the full column aggregate floating point statistics',
    'aggMean': 'Computes the column aggregate mean',
    'aggDataCells': 'Computes the number of non-NoData cells in a column',
    'aggNoDataCells': 'Computes the number of NoData cells in a column',
    'tileMean': 'Compute the Tile-wise mean',
    'tileSum': 'Compute the Tile-wise sum',
    'tileMin': 'Compute the Tile-wise minimum',
    'tileMax': 'Compute the Tile-wise maximum',
    'tileStats': 'Compute the Tile-wise floating point statistics',
    'renderAscii': 'Render ASCII art of tile',
    'noDataCells': 'Count of NODATA cells',
    'dataCells': 'Count of cells with valid data',
    'localAdd': 'Add two Tiles',
    'localSubtract': 'Subtract two Tiles',
    'localMultiply': 'Multiply two Tiles',
    'localDivide': 'Divide two Tiles',
}


__all__ = list(_rf_column_functions.keys()) + list(_rf_unique_functions.keys())


def _create_column_function(name, doc=""):
    """ Create a mapping to Scala UDF for a column function by name"""
    def _(*args):
        jfcn = getattr(_checked_context(), name)
        jcols = [_to_java_column(arg) for arg in args]
        return Column(jfcn(*jcols))
    _.__name__ = name
    _.__doc__ = doc
    _.__module__ = 'pyrasterframes'
    return _


def _register_functions():
    """ Register each function in the scope"""
    for name, doc in _rf_column_functions.items():
        globals()[name] = _create_column_function(name, doc)

    for name, func in _rf_unique_functions.items():
        globals()[name] = func


_register_functions()
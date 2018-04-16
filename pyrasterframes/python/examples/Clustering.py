
#examples_setup
from examples import resource_dir
#examples_setup

#py_cl_imports
from pyrasterframes import *
from pyrasterframes.rasterfunctions import *
from pyspark.sql import *
#py_cl_imports

#py_cl_create_session
spark = SparkSession.builder. \
    master("local[*]"). \
    appName("RasterFrames"). \
    config("spark.ui.enabled", "false"). \
    getOrCreate(). \
    withRasterFrames()
#py_cl_create_session

# The first step is to load multiple bands of imagery and construct
# a single RasterFrame from them.
filenamePattern = "L8-B{}-Elkton-VA.tiff"
bandNumbers = range(1, 8)

# For each identified band, load the associated image file
from functools import reduce
joinedRF = reduce(lambda rf1, rf2: rf1.asRF().spatialJoin(rf2.asRF()),
                  map(lambda bf: spark.read.geotiff(bf[1]) \
                      .withColumnRenamed('tile', 'band_{}'.format(bf[0])),
                  map(lambda b: (b, resource_dir.joinpath(filenamePattern.format(b)).as_uri()), bandNumbers)))

# We should see a single spatial_key column along with columns of tiles.
joinedRF.printSchema()

"""
// SparkML requires that each observation be in its own row, and those
// observations be packed into a single `Vector`. The first step is to
// "explode" the tiles into a single row per cell/pixel
val exploder = new TileExploder()

// To "vectorize" the the band columns we use the SparkML `VectorAssembler`
val assembler = new VectorAssembler()
.setInputCols(bandColNames)
.setOutputCol("features")

// Configure our clustering algorithm
val k = 5
val kmeans = new KMeans().setK(k)

// Combine the two stages
val pipeline = new Pipeline().setStages(Array(exploder, assembler, kmeans))

// Compute clusters
val model = pipeline.fit(joinedRF)

// Run the data through the model to assign cluster IDs to each
val clustered = model.transform(joinedRF)
clustered.show(8)

// If we want to inspect the model statistics, the SparkML API requires us to go
// through this unfortunate contortion:
val clusterResults = model.stages.collect{ case km: KMeansModel ⇒ km}.head

// Compute sum of squared distances of points to their nearest center
val metric = clusterResults.computeCost(clustered)
println("Within set sum of squared errors: " + metric)

val tlm = joinedRF.tileLayerMetadata.left.get

val retiled = clustered.groupBy($"spatial_key").agg(
    assembleTile(
$"column_index", $"row_index", $"prediction",
                                tlm.tileCols, tlm.tileRows, ByteConstantNoDataCellType)
)

val rf = retiled.asRF($"spatial_key", tlm)

val raster = rf.toRaster($"prediction", 186, 169)

val clusterColors = IndexedColorMap.fromColorMap(
    ColorRamps.Viridis.toColorMap((0 until k).toArray)
)

raster.tile.renderPng(clusterColors).write("clustered.png")"""

spark.stop()
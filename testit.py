from pyrasterframes import *
spark.withRasterFrames()
rf = spark.rf.readGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff")
print(rf.tileColumns())
print(rf.spatialKeyColumn())
print(rf.temporalKeyColumn())
rf.select(tileMean("tile")).show()

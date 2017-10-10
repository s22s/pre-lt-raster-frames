# Release Notes

## 0.5.6

* `TileUDF`s are encoded using directly into Catalyst--without Kryo--resulting in an insane
 decrease in serialization time for small tiles (`int8`, <= 128²), and pretty awesome speedup for
 all other cell types other than `float32` (marginal slowing). While not measured, memory 
 footprint is expected to have gone down.


## 0.5.5

* `aggStats` and `tileMean` functions rewritten to compute simple statistics directly rather than using `StreamingHistogram`.
* `tileHistogramDouble` and `tileStatsDouble` were replaced by `tileHistogram` and `tileStats`.
* Added `tileSum`, `tileMin` and `tileMax` functions. 
* Added `aggMean`, `aggDataCells` and `aggNoDataCells` aggregate functions.
* Added `localAggDataCells` and `localAggNoDataCells` cell-local (tile generating) fuctions
* Overflow fix in `LocalStatsAggregateFunction`.

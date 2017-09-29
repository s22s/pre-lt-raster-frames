enablePlugins(BenchmarkPlugin)

jmhIterations := Some(5)
jmhTimeUnit := Some("ms")

// To enable profiling:
//jmhExtraOptions := Some("-prof jmh.extras.JFR")

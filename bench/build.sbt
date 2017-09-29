enablePlugins(BenchmarkPlugin)

jmhIterations := Some(5)
jmhTimeUnit := Some("us")

// To enable profiling:
//jmhExtraOptions := Some("-prof jmh.extras.JFR")

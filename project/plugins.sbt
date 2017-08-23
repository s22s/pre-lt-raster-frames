
resolvers += Resolver.bintrayRepo("hseeberger", "sbt-plugins")

logLevel := sbt.Level.Error

addSbtPlugin("me.lessis" % "bintray-sbt" % "0.3.0")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "1.8.0")

addSbtPlugin("org.tpolecat" % "tut-plugin" % "0.5.2")

addSbtPlugin("com.servicerocket" % "sbt-git-flow" % "0.1.2")

// addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.3.0")

//addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.2")

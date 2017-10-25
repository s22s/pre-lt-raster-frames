#!/bin/bash -e
if [[ ! -d jars ]]; then
  (cd ..; sbt "set retrieveManaged := true" "package")
  mkdir _jars
  mv `find ../lib_managed -name '*.jar'` _jars
  rm -r ../lib_managed
  cp `find ../target -name 'raster-frames_2.11-*.jar'` _jars
  mv _jars jars
fi

exec scala -J-Xmx6G -classpath 'jars/*' -Dscala.color -language:_ -nowarn -i REPLesent.scala -i init.scala

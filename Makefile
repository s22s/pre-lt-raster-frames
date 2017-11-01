.PHONY: docs clean clean-docs

clean-docs: py-clean
	JAVA_OPTS="-Xmx10G" sbt clean makeSite

docs:
	JAVA_OPTS="-Xmx10G" sbt makeSite

clean: py-clean
	sbt clean

py-clean:
	(cd src/main/python; python setup.py clean)

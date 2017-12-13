TOP := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

HAIL_VERSION = devel
GIT_HASH = $(shell git rev-parse --short=12 HEAD)
HAIL_RELEASE = $(HAIL_VERSION)-$(GIT_HASH)

ifndef SPARK_HOME
$(error SPARK_HOME not set)
endif

.PHONY: nativelib
nativelib:
	make -C src/main/c

.PHONY: testnativelib
testnativelib:
	make -C src/main/c test

.PHONY: testng
testng:
	./gradlew test

.PHONY: hailjar
hailjar:
	./gradlew shadowJar

PY4J_ZIP = $(wildcard $(SPARK_HOME)/python/lib/py4j-*-src.zip)

.PHONY: testpy
testpy: hailjar
	SPARK_HOME=$(SPARK_HOME) \
          PYTHONPATH=$(TOP)/python:$(SPARK_HOME)/python:$(PY4J_ZIP) \
	  SPARK_CLASSPATH=$(TOP)/build/libs/hail-all-spark.jar \
	  python -m unittest hail.api1.tests hail.api2.tests hail.typecheck.tests

.PHONY: test
test: testnativelib testpy testng

copypy:
	mkdir -p build/tmp
	rm -rf build/tmp/python
	cp -a python build/tmp

build/tmp/python/hail/docs/distLinks.rst:
	mkdir -p build/tmp/python/hail/docs
	rm -f build/tmp/python/hail/docs/distLinks.rst
	echo "Hail uploads distributions to Google Storage as part of our continuous integration suite." >> $@
	echo "You can download a pre-built distribution from the below links. Make sure you download the distribution that matches your Spark version!" >> $@
	echo >> $@
	for i in 2.0.2 2.1.0; do \
  echo "- \`Current distribution for Spark $i <https://storage.googleapis.com/hail-common/distributions/$(HAIL_VERSION)/Hail-$(HAIL_VERSION)-$(GIT_HASH)-Spark-$1.zip>\`_" >> $@; \
done

python/hail/docs/functions.rst: copypy hailjar
	mkdir -p build/tmp
	(cd build/tmp && SPARK_HOME=$(SPARK_HOME) \
  PYTHONPATH=$(TOP)/python:$(SPARK_HOME)/python:$(PY4J_ZIP) \
  SPARK_CLASSPATH=$(TOP)/build/libs/hail-all-spark.jar \
  python -c 'from hail import *; from hail.utils import *; hc = HailContext(); fd = FunctionDocumentation(); fd.types_rst("python/hail/docs/types.rst"); fd.functions_rst("python/hail/docs/functions.rst")')

.PHONY: pydocs
pydocs: python/hail/docs/functions.rst build/tmp/python/hail/docs/distLinks.rst
	PYTHONPATH=$(TOP)/python:$(SPARK_HOME)/python:$(PY4J_ZIP) \
  HAIL_VERSION=$(HAIL_VERSION) \
  HAIL_RELEASE=$(HAIL_RELEASE) \
  make -C build/tmp/python/hail/docs clean html
	mkdir -p build/www/docs
	rm -rf build/www/docs/stable
	mv build/tmp/python/hail/docs/_build/html build/www/docs/stable

.PHONY: copywww
copywww:
	mkdir -p build
	rm -rf build/www
	cp -a www build/www

.PHONY: copypdf
copypdf:
	mkdir -p build/www
	cp python/hail/docs/misc/LeveneHaldane.pdf build/www

.PHONY: website
website: copypdf copywww build/www/index.html build/www/jobs.html pydocs

build/distributions/hail-python.zip:
	mkdir -p build/distributions
	(cd python && zip -r ../build/distributions/hail-python.zip .)

build/tmp/README.html: README.md
	mkdir -p build/tmp
	pandoc -s README.md -f markdown -t html --mathjax --highlight-style=pygments --columns 10000 -o build/tmp/README.html

build/www/index.html: build/tmp/README.html
	mkdir -p build/www
	xsltproc --html -o build/www/index.html www/readme-to-index.xslt build/tmp/README.html

build/tmp/jobs.html: www/jobs.md
	mkdir -p build/tmp
	pandoc -s www/jobs.md -f markdown -t html --mathjax --highlight-style=pygments --columns 10000 -o build/tmp/jobs.html

build/www/jobs.html: build/tmp/jobs.html
	mkdir -p build/www
	xsltproc --html -o build/www/jobs.html www/jobs.xslt build/tmp/jobs.html

.PHONY:
clean:
	rm -rf build
	make -C src/main/c clean

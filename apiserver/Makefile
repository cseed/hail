.PHONY: check build-hail-shadow-jar test

PY_FILES := $(shell find apiserver -iname \*.py -not -exec git check-ignore -q {} \; -print)

flake8-stmp: $(PY_FILES)
	python3 -m flake8 apiserver
	touch $@

pylint-stmp: $(PY_FILES)
	PYTHONPATH=../hail/python:$$PYTHONPATH python3 -m pylint --rcfile ../pylintrc apiserver --score=n
	touch $@

ifneq ($(PY_CHECK),false)
check: flake8-stmp pylint-stmp
else
check:
endif

build-hail-shadow-jar:
	cd ../hail && ./gradlew shadowJar

test: build-hail-shadow-jar check
	./test-apiserver.sh

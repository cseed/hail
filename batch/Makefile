.PHONY: run run-local test-local clean \
  clean-db-production-local clean-db-production clean-db-test-local clean-db-test

PY_FILES = $(shell find batch -iname \*.py -not -exec git check-ignore -q {} \; -print)
PY_CHECKERS = flake8-stmp pylint-stmp

BATCH_PORT ?= 5000

include ../cloud-sql.mk

flake8-stmp: $(PY_FILES)
	python3 -m flake8 batch
	touch $@

pylint-stmp: $(PY_FILES)
	python3 -m pylint --rcfile ../pylintrc batch --score=n
	touch $@

check: $(PY_CHECKERS)

run:
	python3 -c 'import batch.server; batch.server.serve('$(BATCH_PORT)')'

run-local: run-cloud-sql-proxy-batch-test
	CLOUD_SQL_CONFIG_PATH=batch-secrets/batch-test-cloud-sql-config.json \
            BATCH_USE_KUBE_CONFIG=1 \
            python3 -c 'import batch.server; batch.server.serve('$(BATCH_PORT)')'

ifneq ($(PY_CHECK),false)
test-local: check
endif
test-local: install-cloud-sql-proxy batch-secrets/batch-test-cloud-sql-config.json
	POD_NAMESPACE='test' BATCH_USE_KUBE_CONFIG=1 ./test-locally.sh

clean:
	rm -f $(PY_CHECKERS)

clean-db-production-local: batch-secrets/batch-production-cloud-sql-config.json
	CLOUD_SQL_CONFIG_PATH=$< ./clean-db.sh

clean-db-production: /batch-secrets/batch-production-cloud-sql-config.json
	CLOUD_SQL_CONFIG_PATH=$< ./clean-db.sh

clean-db-test-local: batch-secrets/batch-test-cloud-sql-config.json
	CLOUD_SQL_CONFIG_PATH=$< ./clean-db.sh

clean-db-test: /batch-secrets/batch-test-cloud-sql-config.json
	CLOUD_SQL_CONFIG_PATH=$< ./clean-db.sh

all: build

HAIL_ROOT = ../../../..

.PHONY: build
build:
	(cd $(HAIL_ROOT) && ./gradlew installDist && rm build/install/hail/lib/asm-3.1.jar)
	rm -rf hail
	cp -r $(HAIL_ROOT)/build/install/hail .
	mkdir hail/python
	cp -r $(HAIL_ROOT)/python hail
	docker build -t seqrserver .
	docker tag seqrserver gcr.io/seqr-project/seqrserver

.PHONY: push
push:
	gcloud docker -- push gcr.io/seqr-project/seqrserver

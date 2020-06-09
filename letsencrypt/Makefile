include ../config.mk

letsencrypt-pod.yaml letsencrypt.sh: %: %.in
	sed -e "s,@project@,$(PROJECT),g" \
	  -e "s;@domains@;$(shell paste -s -d, domains.txt);g" \
	  -e "s,@domain@,$(DOMAIN),g" \
	  -e "s,@ip@,$(IP),g" \
	  < $< > $@

.PHONY: build
build: letsencrypt.sh
	docker build -t letsencrypt .

.PHONY: push
push: build
	docker tag letsencrypt gcr.io/$(PROJECT)/letsencrypt
	docker push gcr.io/$(PROJECT)/letsencrypt

.PHONY: run
run: letsencrypt-pod.yaml push
	/bin/bash run-letsencrypt.sh

clean:
	rm -rf letsencrypt-pod.yaml letsencrypt.sh

# Convenience targets. Real builds happen via mvn / docker.

VERSION ?= 0.1.0-SNAPSHOT
IMAGE   ?= acceldata/ranger-yunikorn-agent

.PHONY: build test image push clean

build:
	mvn -DskipTests clean package

test:
	mvn test

image: build
	docker build -t $(IMAGE):$(VERSION) .

push: image
	docker push $(IMAGE):$(VERSION)

clean:
	mvn clean
	rm -rf target

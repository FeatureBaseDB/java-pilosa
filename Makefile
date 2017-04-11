
.PHONY: test test-all

build:
	mvn -f com.pilosa.client/pom.xml package

test:
	mvn -f com.pilosa.client/pom.xml test

test-all:
	mvn -f com.pilosa.client/pom.xml integration-test
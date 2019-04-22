
.PHONY: build clean cover doc fast generate test test-all release

build:
	mvn -f com.pilosa.client/pom.xml clean package

clean:
	mvn -f com.pilosa.client/pom.xml clean

doc:
	mvn -f com.pilosa.client/pom.xml javadoc:javadoc

fast:
	mvn -f com.pilosa.client/pom.xml clean package -Dskip.tests=true

generate:
	protoc --java_out=com.pilosa.client/src/main/java/ com.pilosa.client/src/internal/public.proto

test:
	mvn -f com.pilosa.client/pom.xml test

test-all:
	mvn -f com.pilosa.client/pom.xml test && mvn -f com.pilosa.client/pom.xml failsafe:integration-test

cover:
	mvn -f com.pilosa.client/pom.xml clean test failsafe:integration-test jacoco:report
	@echo See ./com.pilosa.client/target/site/jacoco/index.html for the coverage report

release:
	mvn -f com.pilosa.client/pom.xml clean deploy -P release

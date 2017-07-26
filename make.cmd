@echo off

REM default target is test
if "%1" == "" (
    goto :test
)

2>NUL call :%1
if errorlevel 1 (
    echo Unknown target: %1
)

goto :end

:build
    mvn -f com.pilosa.client/pom.xml clean package
    goto :end

:clean
    mvn -f com.pilosa.client/pom.xml clean
    goto :end

:cover
    mvn -f com.pilosa.client/pom.xml clean test failsafe:integration-test jacoco:report
    goto :end

:doc
    mvn -f com.pilosa.client/pom.xml javadoc:javadoc
    goto :end

:generate
    echo Generating protobuf code is not supported on this platform.
    goto :end

:test
    mvn -f com.pilosa.client/pom.xml test
    goto :end

:test-all
    mvn -f com.pilosa.client/pom.xml test failsafe:integration-test
    goto :end

:end

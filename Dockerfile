# Java 25 JRE carrying the Java4Shell fix (CVE-2026-114938 / JDK-8393212):
# must be 25.0.5.1 or newer. Pinned to Azul Zulu because the Eclipse Temurin
# Docker images had not yet been rebuilt for 25.0.5.1; once
# eclipse-temurin:25.0.5.1_2-jre is published, it can be used instead.
FROM azul/zulu-openjdk:25.0.5.1-jre

# This Dockerfile is optimized to work together with a GitHub build
# pipeline, which compiles the source code and packs it into a
# JAR in a "target" directory.
#
# If you want to build an image yourself on your own computer, you
# will first need to run:
#
# mvn clean install
#
# Once Maven has completed, you can then run a regular Docker build.

COPY ./target/*.jar ./directory_sync_service.jar

CMD java -jar directory_sync_service.jar


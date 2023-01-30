FROM eclipse-temurin:19.0.1_10-jre-alpine

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


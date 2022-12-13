FROM maven:3.8.6-eclipse-temurin-19-alpine AS build

COPY ./ /workingdir/
WORKDIR /workingdir

RUN ls -la
RUN java -version

RUN mvn clean install

FROM eclipse-temurin:19.0.1_10-jre-alpine

RUN mkdir -p /etc/bridgehead/
RUN chmod -R a+rx /etc/bridgehead/
COPY --from=build /workingdir/src/docker/directory_sync.conf /etc/bridgehead/
#RUN chmod a+r /etc/bridgehead/*
COPY --from=build /workingdir/src/docker/start.sh ./

COPY --from=build /workingdir/target/*.jar ./directory_sync_service.jar

CMD sh ./start.sh


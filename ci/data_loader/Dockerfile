FROM debian:stable

RUN apt-get -y update && apt-get -y install curl

WORKDIR /app

# Get blazectl, for uploading data to a FHIR store
RUN curl -LO https://github.com/samply/blazectl/releases/download/v0.3.0/blazectl-0.3.0-linux-amd64.tar.gz
RUN tar xzf blazectl-0.3.0-linux-amd64.tar.gz
RUN mv ./blazectl /usr/local/bin/blazectl
RUN blazectl --version

# Clean up
RUN rm blazectl-0.3.0-linux-amd64.tar.gz
RUN apt-get purge -y curl
RUN apt-get -y autoremove

# Set up run environment
RUN mkdir -p /app
#RUN echo ##########################################
#RUN pwd
#RUN ls -la run.sh
#RUN echo ##########################################
COPY ./run.sh /app/run.sh
RUN chmod a+rx /app/run.sh

CMD /app/run.sh


# Directory sync service

Runs a Directory sync service at a biobank site.

This service keeps the BBMRI Directory up to date with the number of samples, etc.
kept in the biobank. It also updates the local FHIR store with the latest contact
details etc. from the Directory.

It is implemented as a standalone component that encapsulates the [Directory sync library](https://github.com/samply/directory-sync).

It is assumed that you have a local FHIR store containing information about patients
and samples, as well as the details of your biobank. The data in this store should conform to
the [GBA profile](https://simplifier.net/bbmri.de/~resources?category=Profile).

If you are running a [Bridgehead](https://github.com/samply/bridgehead), then
you will already have a suitable store, running under [Blaze](https://github.com/samply/blaze).

The service uses Quartz to start synchronization at regular intervals. These are specified
using cron syntax. Once daily is the recommended frequency.

You need to provide URL, user and password for the Directory.

Additionally, you will need to supply a URL for the local FHIR store.

See below for ways to get these parameters to the application.

## Building

If you are not using Docker, then you will need to install Java 19 and Maven and then run:

mvn clean install

## Running

If you are not using Docker, proceed as follows.

First, you need to provide the necessary parameters via a file, which
resides in:

/etc/bridgehead/directory_sync.conf

The contents of the file should look something like this:

directory_sync.directory.url=https://bbmritestnn.gcc.rug.nl
directory_sync.directory.user_name=testuser@gmail.com
directory_sync.directory.pass_code=KJNJFZTIUZBUZbzubzubzbfdeswsqaq
directory_sync.fhir_store_url=http://store:8080
directory_sync.timer_cron=

Once you have done this, you can start the service directly from the command line:

java -jar target/directory_sync_service\*.jar

## Docker

Docker provides an alternative way of building and running Directory sync.

First, you will need to set up the environment variables
for this. The following table shows the variables that you will need:

|Variable           |Purpose                                           |
|:------------------|:-------------------------------------------------|
|DIRECTORY_URL      |Base URL of the Directory                         |
|DIRECTORY_USER_NAME|User name for logging in to Directory             |
|DIRECTORY_PASS_CODE|Password for logging in to Directory              |
|FHIR_STORE_URL     |URL for Bridgehead Blaze store                    |
|TIMER_CRON         |Execution interval for Directory sync, cron format|

For your convenience, we recommend that you store these in a .env file.
The file could look like this:

DIRECTORY_URL=https://bbmritestnn.gcc.rug.nl
DIRECTORY_USER_NAME=foo@gmail.com
DIRECTORY_PASS_CODE=qwelmqwldmqwklmdLKJNJKNKJN
FHIR_STORE_URL=http://store:8080
TIMER_CRON="0/20 * * * * ?"

To create a Docker image, run the following from a command line:

docker build -t samply/directory_sync_service . --no-cache

To run from Docker, you can use Docker run:

docker run samply/directory_sync_service

Alternatively, you can use docker-compose:

docker-compose up

## License
        
Copyright 2022 The Samply Community
        
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
        
http://www.apache.org/licenses/LICENSE-2.0
        
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 

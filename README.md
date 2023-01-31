# Directory sync service

Runs a Directory sync service at a biobank site.

This service keeps the [BBMRI Directory](https://directory.bbmri-eric.eu/) up to date with the number of samples, etc.
kept in the biobank. It also updates the local FHIR store with the latest contact
details etc. from the Directory.

It is implemented as a standalone component that encapsulates the [Directory sync library](https://github.com/samply/directory-sync).

It is assumed that you have access to a FHIR store containing information about patients
and samples, as well as the details of your biobank. The data in this store should conform to
the [GBA profile](https://simplifier.net/bbmri.de/~resources?category=Profile).

If you are running a [Bridgehead](https://github.com/samply/bridgehead), then
you will already have a suitable store, running under [Blaze](https://github.com/samply/blaze).

The service is able to use Quartz to start synchronization at regular intervals. These are specified
using cron syntax. If no cron information is supplied, Directory sync will only be run once.

You need to provide URL, user and password for the Directory.

Additionally, you will need to supply a URL for the local FHIR store.

See below for ways to get these parameters to the application.

## Usage with Docker

We recommend using Docker for running Directory sync.

First, you will need to set up the environment variables for this:

|Variable              |Purpose                                           |Default if not specified          |
|:---------------------|:-------------------------------------------------|:---------------------------------|
|DS_DIRECTORY_URL      |Base URL of the Directory                         |https://bbmritestnn.gcc.rug.nl    |
|DS_DIRECTORY_USER_NAME|User name for logging in to Directory             |                                  |
|DS_DIRECTORY_PASS_CODE|Password for logging in to Directory              |                                  |
|DS_FHIR_STORE_URL     |URL for FHIR store                                |http://store:8080/fhir            |
|DS_TIMER_CRON         |Execution interval for Directory sync, cron format|                                  |
|DS_RETRY_MAX          |Maximum number of retries when sync fails         |10                                |
|DS_RETRY_INTERVAL     |Interval between retries (seconds)                |20 seconds                        |

DS_DIRECTORY_USER_NAME and DS_DIRECTORY_PASS_CODE are mandatory. If you do not specify these,
Directory sync will not run. Contact [Directory admin](directory@helpdesk.bbmri-eric.eu) to get login credentials.

If DS_TIMER_CRON is not specified, Directory sync will be executed once, and then the
process will terminate.

The DS_RETRY\_ variables specify how Directory sync reacts to failure. RETRY_MAX should
be greater than zero. A typical case where retries may be necessary is when
you simultaneously start Directroy sync and FHIR store. It takes some time
for a FHIR store to start, whereas Directory sync starts immediately, so
Directory sync needs to have a way to poll the store until is ready.

For your convenience, we recommend that you store these in a .env file.
The file could look like this:

```
DS_DIRECTORY_URL=https://bbmritestnn.gcc.rug.nl
DS_DIRECTORY_USER_NAME=foo@foomail.com
DS_DIRECTORY_PASS_CODE=qwelmqwldmqwklmdLKJNJKNKJN
DS_FHIR_STORE_URL=http://store:8080/fhir
```

With this configuration, the synchronization will be done just once, with the test
Directory server. The login details are fake, you will need to replace them with something
sensible.

To run from Docker:

```
docker run --env-file .env samply/directory_sync_service
```

Alternatively, you can use docker-compose. An example docker-compose.yml file has been
included in this repository. To start:

```
docker-compose up
```

## Standalone build and run

You can also build and run this service on your own computer, if Docker is not an option.

### Prerequisites

Please run from within Linux.

You will need to install git, Java 19 and Maven. Clone this repository and cd into it.

### Building

From the command line, run:

```
mvn clean install
```

### Running

You need to provide the necessary parameters via environment variables, as documented above. These should be set in the same shell as the one that you use to run java.

Once you have done this, you can start the service directly from the command line:

```
java -jar target/directory_sync_service\*.jar
```

## License
        
Copyright 2022 The Samply Community
        
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
        
http://www.apache.org/licenses/LICENSE-2.0
        
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 

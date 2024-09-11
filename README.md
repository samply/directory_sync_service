# Directory sync service

Runs a Directory sync service at a biobank site.

This service keeps the [BBMRI Directory](https://directory.bbmri-eric.eu/) up to date with the number of samples, etc.
kept in the biobank. It also updates the local FHIR store with the latest contact
details etc. from the Directory.

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

## Collections

It is generally advised that you assign collections to all Specimens. This is best achieved
using an extension to the Specimen resource, which contains a reference to a collection,
see the FHIR profile. The IDs of these collections
will be passed on to the Directory during synchronization.

However, for some biobanks
it may not be feasible to assign individual specimens to a collection. A fallback
solution is available in this case: the default collection ID. This is a single ID
that will be assigned to all specimens without collection references. There are two possible
ways to specify this ID: either as environment variable (see DS_DIRECTORY_DEFAULT_COLLECTION_ID
in the table below) or as an orphan collection in FHIR that has no references from
any specimens but whose identifier is a collection ID. 

## Usage with Docker

We recommend using Docker for running Directory sync.

First, you will need to set up the environment variables for this:

| Variable                           | Purpose                                                                                                                                                              | Default if not specified               |
|:-----------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------|
| DS_DIRECTORY_URL                   | Base URL of the Directory                                                                                                                                            | https://directory-backend.molgenis.net |
| DS_DIRECTORY_USER_NAME             | User name for logging in to Directory                                                                                                                                |                                        |
| DS_DIRECTORY_USER_PASS             | Password for logging in to Directory                                                                                                                                 |                                        |
| DS_DIRECTORY_DEFAULT_COLLECTION_ID | ID of collection to be used if not in samples                                                                                                                        |                                        |
| DS_DIRECTORY_MIN_DONORS            | Minimum number of donors per star model hypercube                                                                                                                    | 10                                     |
| DS_DIRECTORY_MAX_FACTS             | Max number of star model hypercubes to be generated                                                                                                                  |                                        |
| DS_DIRECTORY_ALLOW_STAR_MODEL      | Set to 'True' to send star model info to Directory                                                                                                                   | False                                  |
| DS_DIRECTORY_MOCK                  | Set to 'True' mock a Directory. In this mode, directory-sync will not contact the Directory. All Directory-related methods will simply return plausible fake values. | False                                  |
| DS_FHIR_STORE_URL                  | URL for FHIR store                                                                                                                                                   | http://bridgehead-bbmri-blaze:8080     |
| DS_TIMER_CRON                      | Execution interval for Directory sync, cron format                                                                                                                   |                                        |
| DS_RETRY_MAX                       | Maximum number of retries when sync fails                                                                                                                            | 10                                     |
| DS_RETRY_INTERVAL                  | Interval between retries (seconds)                                                                                                                                   | 20 seconds                             |

DS_DIRECTORY_USER_NAME and DS_DIRECTORY_USER_PASS are mandatory. If you do not specify these,
Directory sync will not run. Contact [Directory admin](directory@helpdesk.bbmri-eric.eu) to get login credentials.

DS_DIRECTORY_DEFAULT_COLLECTION_ID is not mandatory, but you will need to specify it if your ETL does not assign Directory collection IDs to specimens.

If you set DS_DIRECTORY_ALLOW_STAR_MODEL to 'True', then the star model summary information
for your data will be generated and sent to the Directory. You are advised to talk to
your local data protection group before doing this.

If DS_TIMER_CRON is not specified, Directory sync will be executed once, and then the
process will terminate.

The DS_RETRY\_ variables specify how Directory sync reacts to failure. RETRY_MAX should
be greater than zero. A typical case where retries may be necessary is when
you simultaneously start Directroy sync and FHIR store. It takes some time
for a FHIR store to start, whereas Directory sync starts immediately, so
Directory sync needs to have a way to poll the store until is ready.

If DS_DIRECTORY_MOCK is set to 'True', the Directory will not be contacted, but you will still need to specify login credentials (which will then be ignored).

For your convenience, we recommend that you store these variables in a .env file.
The file could look like this:

```
DS_DIRECTORY_URL=https://bbmritestnn.gcc.rug.nl
DS_DIRECTORY_USER_NAME=foo@foomail.com
DS_DIRECTORY_USER_PASS=qwelmqwldmqwklmdLKJNJKNKJN
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
        
Copyright 2024 The Samply Community
        
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
        
http://www.apache.org/licenses/LICENSE-2.0
        
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 

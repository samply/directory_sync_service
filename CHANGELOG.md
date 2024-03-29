# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [0.0.1 - 2022-12-13]
### Added
- All files from initial implementation of this component.

## [0.1.0 - 2022-12-13]
### Added
- Parameter sanity checking

## [0.1.1 - 2022-12-14]
### Changed
- Dependency versions updated in pom.xml

## [0.2.0 - 2022-12-14]
### Added
- Added basic GitHub workflow

## [0.3.0 - 2022-12-14]
### Added
- Added sensible defaults for parameters.

## [0.3.1 - 2022-12-14]
### Changed
- Initialize the link to CQL Directory sync was trying to execute CQL, but not finding it. The change, suggested by Alex, fixed the problem.

## [1.0.0 - 2022-12-20]
### Changed
- Public release version of code.
- Tested against a live Directory.
### Added
- Added GitHub CI.

## [1.0.1 - 2022-12-21]
### Changed
- Upgraded all GitHub Docker actions to latest versions

## [1.0.3 - 2022-12-21]
### Changed
- Changes made to simplify GitHub release process.

## [1.1.0 - 2022-12-23]
### Added
- Added single-shot functionality. Allows Directory sync to be run just once.

## [1.1.2 - 2022-12-23]
### Added
- Added push-to-DockerHub for README

## [1.1.3 - 2022-12-23]
### Changed
- README push to DockerHub trigger changed to release

## [1.1.4 - 2023-01-25]
### Changed
- Allow standard cron definitions, allow safe failover if store is not yet running.

## [1.2.0 - 2023-01-30]
### Changed
- All environment variables now start with "DS\_", giving them a namespace, useful in the context of a Bridgehead.
- The File-based configuration mechanism has been replaced with a Spring Boot configuration, which means that sensitive information like passwords is no longer stored in a file.


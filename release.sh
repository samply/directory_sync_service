# Create a new GitHub release, based on project version in pom.xml.
#
# Works by pushing a v*.*.* tag to GitHub. This is caught by
# the release pipeline and causes a new release to be generated.
# A release number image will also be created on Docker Hub.
#
# N.B you need to have Maven installed.

PROJECT_VERSION=$( mvn help:evaluate -Dexpression=project.version -q -DforceStdout )
GIT_TAG=v${PROJECT_VERSION}

git tag $GIT_TAG
git push origin $GIT_TAG


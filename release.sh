# Create a new GitHub release, based on project version in pom.xml.
#
# Works by pushing a v*.*.* tag to GitHub. This is caught by
# the release pipeline and causes a new release to be generated.
# A release number image will also be created on Docker Hub.
#
# N.B you need to have Maven installed.

# Fail early, before any git operations, if Maven is not available.
if ! command -v mvn >/dev/null 2>&1; then
  echo "WARNING: mvn (Maven) was not found on the PATH; cannot determine the project version." >&2
  echo "No git operations were performed. Install Maven and re-run this script." >&2
  exit 1
fi

PROJECT_VERSION=$( mvn help:evaluate -Dexpression=project.version -q -DforceStdout )

# Bail out if the version lookup failed (e.g. Maven error), otherwise an
# empty version would produce a bogus "v" tag below.
if [ -z "$PROJECT_VERSION" ]; then
  echo "WARNING: could not determine the project version from pom.xml." >&2
  echo "No git operations were performed. Check the Maven output above and re-run this script." >&2
  exit 1
fi

GIT_TAG=v${PROJECT_VERSION}

echo GIT_TAG=$GIT_TAG

git tag $GIT_TAG
git push origin $GIT_TAG


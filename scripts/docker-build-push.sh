#!/usr/bin/env bash
# Build the community-search Docker image locally and push to Docker Hub.
# Usage: ./scripts/docker-build-push.sh [tag]
#   - default tag: the version in Cargo.toml, plus "latest"
#   - override repo via DOCKERHUB_REPO env var (default: payneio/community-search)
#   - run `docker login` first
set -euo pipefail

REPO="${DOCKERHUB_REPO:-payneio/community-search}"
PLATFORM="linux/amd64"

cd "$(dirname "$0")/.."

if [ $# -ge 1 ]; then
  VERSION="$1"
else
  VERSION=$(grep -m1 '^version' Cargo.toml | sed -E 's/version = "(.*)"/\1/')
fi

if [ -z "$VERSION" ]; then
  echo "ERROR: could not determine version" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: docker daemon not reachable" >&2
  exit 1
fi

echo "Building $REPO:$VERSION ($PLATFORM)..."
docker build \
  --platform "$PLATFORM" \
  --tag "$REPO:$VERSION" \
  --tag "$REPO:latest" \
  .

echo "Pushing $REPO:$VERSION and $REPO:latest..."
docker push "$REPO:$VERSION"
docker push "$REPO:latest"

echo "Done: $REPO:$VERSION, $REPO:latest"

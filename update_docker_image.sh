#!/bin/bash
set -e

# DEVELOPER: update this field anytime you make a new docker image
VERSION="0.4"
BUILD_TAG="us-east4-docker.pkg.dev/broad-dsp-monster-hca-dev/monster-dev-env/hca_ingest_compose_dev_env:${VERSION}"

echo "using build tag: ${BUILD_TAG}"

if docker manifest inspect $BUILD_TAG > /dev/null ;
then
    echo "Docker image already exists. Do you want to overwrite this tag?."
    read -r -p "Press [Enter] to overwrite or [Ctrl+C] to exit and change the version number."
fi

echo "Building and pushing new image ${BUILD_TAG}"

#docker build . -t ${BUILD_TAG}
#docker push "${BUILD_TAG}"

#!/bin/bash
BRANCH_NAME=$(git symbolic-ref --short HEAD)
VERSION_DATE=$(date '+%Y-%m-%d_%H%M')
IMAGENAME="prodapt/dmf:${BRANCH_NAME}-${VERSION_DATE}"


docker build --network=host -t ${IMAGENAME} .

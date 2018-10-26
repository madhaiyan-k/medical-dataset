#!/bin/bash
# A small shell script which builds the project

usage() {
    echo "build.sh [-s][-h]"
    echo "Build and the program"
    echo "-s: skip tests"
    echo "-h: help"
    exit 1
}

mydir="$(dirname "$0")"

# Bail out on first error; verbose
set -x
set -e

SKIP="0"
while getopts sh FLAG; do
   case $FLAG in
      s) SKIP="1"
         ;;
      *) usage
         ;;
   esac
done

EXTRAARGS=""
if [ $SKIP -eq "1" ]; then
    EXTRAARGS="-DskipTests "$EXTRAARGS
fi
pushd $mydir/../
mvn $EXTRAARGS clean install
popd

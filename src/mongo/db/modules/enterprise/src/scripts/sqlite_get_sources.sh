#!/bin/bash

set -o verbose
set -o errexit

# This script downloads and imports sqlite
# This script is designed to run on Linux or Mac OS X
#
# sqlite tarballs use the name "sqlite-autoconf-version" so we need to rename it
#

VERSION=3190300
NAME=sqlite
TARBALL=$NAME-autoconf-$VERSION.tar.gz 
TARBALL_DEST_DIR=$NAME-autoconf-$VERSION
DEST_DIR=`git rev-parse --show-toplevel`/src/sqlite

if [ ! -f $TARBALL ]; then
    echo "Get tarball"
    wget https://sqlite.org/2017/$TARBALL
fi

tar -zxvf $TARBALL

mv $DEST_DIR/SConscript $TARBALL_DEST_DIR/SConscript 

rm -rf $DEST_DIR

mv $TARBALL_DEST_DIR $DEST_DIR

rm $TARBALL

# Note: There are no config.h or other build artifacts to generate
echo "Done"

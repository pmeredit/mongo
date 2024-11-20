#!/bin/bash
set -e

# This script creates a copy of sources for mongocxx and mongoc.
# This script currently only works on Linux x86_64 platforms.

if [ "$#" -ne 0 ]; then
	echo "This script does not take any arguments"
	exit 1
fi

# Create a temporary directory to clone and configure mongocxx
TEMP_DIR=/tmp/mongocxx
rm -rf $TEMP_DIR
mkdir $TEMP_DIR

# Setup the directory names
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
BASE_DIR="$SCRIPT_DIR/.."
mkdir -p $BASE_DIR
DIST_DIR=$BASE_DIR/dist
linuxPlatform=linux_x86_64
aarchPlatform=linux_aarch64
PLATFORM_DIR="$DIST_DIR/platform"

# Copy existing platform files. For future version upgrades, we may have to run the cmake invocation
# to configure the mongoc/mongocxx/bsoncxx repos instead of reusing existing platform files.
TEMP_BSONCXX_PLATFORM_DIR=/tmp/existing/bsoncxx
rm -rf $TEMP_BSONCXX_PLATFORM_DIR
for platform in $linuxPlatform $aarchPlatform
do
  mkdir -p $TEMP_BSONCXX_PLATFORM_DIR/$platform
  cp -r $PLATFORM_DIR/$platform/bsoncxx $TEMP_BSONCXX_PLATFORM_DIR/$platform
done

TEMP_MONGOCXX_PLATFORM_DIR=/tmp/existing/mongocxx/
rm -rf $TEMP_MONGOCXX_PLATFORM_DIR
for platform in $linuxPlatform $aarchPlatform
do
  mkdir -p $TEMP_MONGOCXX_PLATFORM_DIR/$platform
  cp -r $PLATFORM_DIR/$platform/mongocxx $TEMP_MONGOCXX_PLATFORM_DIR/$platform
done

# Clean the output directories
rm -rf $DIST_DIR
rm -rf $PLATFORM_DIR

cd $TEMP_DIR

# Download the 1.24.3 release of mongoc
wget https://github.com/mongodb/mongo-c-driver/releases/download/1.24.3/mongo-c-driver-1.24.3.tar.gz
tar xzf mongo-c-driver-1.24.3.tar.gz
cd mongo-c-driver-1.24.3
mkdir cmake-build
cd cmake-build
# Generate platform specific files
# TODO(SERVER-77996): selectively enable HAVE_ANS1_STRING_GET0_DATA whenever it is available on the platform.
cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF -DENABLE_ZLIB=OFF -DENABLE_ICU=OFF -DHAVE_ASN1_STRING_GET0_DATA=0 ..
cd ..

# Copy the mongoc source files
mkdir -p $DIST_DIR/mongoc
cp -r src/libmongoc/src/mongoc/* $DIST_DIR/mongoc
mkdir -p $DIST_DIR/mongoc/common
cp -r src/common/*.h $DIST_DIR/mongoc/common
mkdir -p $DIST_DIR/mongoc/kms_message
cp -r src/kms-message/src/kms_message/* $DIST_DIR/mongoc/kms_message

# Copy libbson header files
mkdir -p $DIST_DIR/bson
cp -r src/libbson/src/bson/*.h $DIST_DIR/bson

# Copy the mongoc platform specified files
for platform in $linuxPlatform $aarchPlatform
do
  mkdir -p $PLATFORM_DIR/$platform/mongoc
  cp cmake-build/src/libmongoc/src/mongoc/mongoc-config.h $PLATFORM_DIR/$platform/mongoc
done

# Copy the mongoc licences
cp THIRD_PARTY_NOTICES $DIST_DIR/mongoc
cp COPYING $DIST_DIR/mongoc

# Download the 3.7.2 release of mongocxx
cd $TEMP_DIR
curl -OL https://github.com/mongodb/mongo-cxx-driver/releases/download/r3.7.2/mongo-cxx-driver-r3.7.2.tar.gz
tar -xzf mongo-cxx-driver-r3.7.2.tar.gz
cd mongo-cxx-driver-r3.7.2/build
# TODO Get this cmake invocation to work.
#cmake .. -DCMAKE_BUILD_TYPE=Release -DBSONCXX_POLY_USE_MNMLSTC=0 -DBSONCXX_POLY_USE_BOOST=1 -DENABLE_TESTS=OFF
cd ..

# Copy the bsoncxx source files
mkdir -p $DIST_DIR/bsoncxx
cp -r src/bsoncxx/* $DIST_DIR/bsoncxx
# Remove the test directories. Don't remove test_util directory as it's included 
# in the main source files.
rm -rf $DIST_DIR/bsoncxx/test
# Copy the platform specific bsoncxx source files
for platform in $linuxPlatform $aarchPlatform
do 
  mkdir -p $PLATFORM_DIR/$platform/bsoncxx/config/private
  cp -r $TEMP_BSONCXX_PLATFORM_DIR/$platform/bsoncxx/config/* $PLATFORM_DIR/$platform/bsoncxx/config
done 

# TODO Once we can run the project cmake properly, copy the generated platform files
#cp build/src/bsoncxx/config/config.hpp $PLATFORM_DIR/bsoncxx/config
#cp build/src/bsoncxx/config/export.hpp $PLATFORM_DIR/bsoncxx/config
#cp build/src/bsoncxx/config/version.hpp $PLATFORM_DIR/bsoncxx/config
#mkdir $PLATFORM_DIR/bsoncxx/config/private
#cp build/src/bsoncxx/config/private/config.hh $PLATFORM_DIR/bsoncxx/config/private 

# Copy the mongocxx source files
mkdir -p $DIST_DIR/mongocxx
cp -r src/mongocxx/* $DIST_DIR/mongocxx
# Remove the test directories. Don't remove test_util directory as it's included 
# in the main source files.
rm -rf $DIST_DIR/mongocxx/test
# Copy the platform specific mongocxx source files
for platform in $linuxPlatform $aarchPlatform
do
  mkdir -p $PLATFORM_DIR/$platform/mongocxx/config/private
  cp -r $TEMP_MONGOCXX_PLATFORM_DIR/$platform/mongocxx/config/* $PLATFORM_DIR/$platform/mongocxx/config
done

#cp build/src/mongocxx/config/config.hpp $PLATFORM_DIR/mongocxx/config
#cp build/src/mongocxx/config/export.hpp $PLATFORM_DIR/mongocxx/config
#cp build/src/mongocxx/config/version.hpp $PLATFORM_DIR/mongocxx/config
#mkdir $PLATFORM_DIR/mongocxx/config/private
#cp build/src/mongocxx/config/private/config.hh $PLATFORM_DIR/mongocxx/config/private

# Copy the mongocxx licences
cp THIRD-PARTY-NOTICES $DIST_DIR/mongocxx
cp LICENSE $DIST_DIR/mongocxx

# Cleanup files that are not used
cd $DIST_DIR
find . -type f -not \( -iname "*.cpp" -o -iname "*.hpp" -o -iname "*.h" -o -iname "*.hh" -o -iname "*.c" -o -iname "*.def" -o -iname "*.defs" -o -iname "COPYING" -o -iname "THIRD_PARTY_NOTICES" -o -iname "LICENSE" -o -iname "THIRD-PARTY-NOTICES" \) -exec rm {} \;

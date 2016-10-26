#!/bin/bash

WORK_ROOT=$(dirname $0)
cd $WORK_ROOT
WORK_ROOT=$(pwd)

ARCH=$(uname -m)
BUILD_ROOT=${WORK_ROOT}/..
cd ${BUILD_ROOT}/_CPack_Packages/Darwin-x86_64/productbuild/hpccsystems-clienttools-community*${ARCH}
PACKAGE_SRC=$(pwd)
cd $WORK_ROOT

#-----------------------------------------------
#
# Get variables
#
#-----------------------------------------------
#get os "Darwin"
OSTYPE=Darwin

#ID=$(grep "^SET\(CPACK_PACKAGE_NAME \"hpccsystems-clienttools-6.2\"" ${BUILD_ROOT}/CPackConfig.cmake)
ID=$(grep "CPACK_PACKAGE_NAME " ${BUILD_ROOT}/CPackConfig.cmake | \
     cut -d' ' -f2 | cut -d'"' -f2)
echo "Title: $ID"

PACKAGE_NAME=$(grep "CPACK_SOURCE_PACKAGE_FILE_NAME " \
       ${BUILD_ROOT}/CPackConfig.cmake | cut -d' ' -f2 | cut -d'"' -f2)
PACKAGE_NAME="${PACKAGE_NAME}${OSTYPE}-${ARCH}"
echo "CPACK_PACKAGE_NAME: $PACKAGE_NAME"

FULL_VERSION=$(grep "CPACK_PACKAGE_VERSION " ${BUILD_ROOT}/CPackConfig.cmake | \
     cut -d' ' -f2 | cut -d'"' -f2)
echo "FULL_VERSION: $FULL_VERSION"

#-----------------------------------------------
#
# Create package with pkgbuilds
#
#-----------------------------------------------
echo ""
echo "Create ${PACKAGE_NAME}.pkg with pkgbuild"
[ -d "${PACKAGE_NAME}" ] && rm -rf ${PACKAGE_NAME}
mkdir -p ${PACKAGE_NAME}
cd ${PACKAGE_NAME}
echo "pkgbuild --root ${PACKAGE_SRC}/opt --install-location "/opt" --identifier $ID ${PACKAGE_NAME}.pkg"
pkgbuild --root ${PACKAGE_SRC}/opt --install-location "/opt" --identifier $ID ${PACKAGE_NAME}.pkg
if [ $? -ne 0 ]
then
  echo "Error to run pkgbuild"
  exit 1
fi
if [ ! -e ${PACKAGE_NAME}.pkg ]
then
  echo "Failed to generate ${PACKAGE_NAME}.pkg"
  exit 1
fi
cd ..

#-----------------------------------------------
#
# Creaate distribution.xml from template
#
#-----------------------------------------------
echo ""
echo "Create Distribution.xml"

sed "s/\${TITLE}/${ID}/g; \
     s/\${PACKAGE_NAME}/${PACKAGE_NAME}/g; \
     s/\${FULL_VERSION}/${FULL_VERSION}/g" \
     ${WORK_ROOT}/distribution_template.xml > distribution.xml

#-----------------------------------------------
#
# Add Welcome/License/ReadMe with productbuild
#
#-----------------------------------------------
echo ""
echo "Add Welcome/License/ReadMe to ${PACKAGE_NAME}.pkg with productbuild"
[ -d resources ] && rm -rf resources
mkdir -p resources
cp ${PACKAGE_SRC}/Contents/*.txt resources/
echo "productbuild --distribution ./distribution.xml --package-path ./${PACKAGE_NAME} --resources ./resources  ${PACKAGE_NAME}.pkg"
productbuild --distribution ./distribution.xml --package-path ./${PACKAGE_NAME} --resources ./resources  ${PACKAGE_NAME}.pkg

#-----------------------------------------------
#
# Create DMG file
#
#-----------------------------------------------
echo ""
mv ${PACKAGE_NAME}.pkg ./${PACKAGE_NAME}/
echo "hdiutil create -volname ${PACKAGE_NAME} -srcfolder ./${PACKAGE_NAME} -ov ${PACKAGE_NAME}.dmg"
hdiutil create -volname ${PACKAGE_NAME} -srcfolder ./${PACKAGE_NAME} -ov ${PACKAGE_NAME}.dmg

echo ""
echo "DONE"

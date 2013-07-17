#!/bin/bash
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################
#
if [ $# -lt 1 ]; then
    echo usage: $0 debfile
    exit 1
fi

FNAME=$1
DNAME=$(basename ${FNAME} .deb)
mkdir -p ${DNAME}/DEBIAN
fakeroot dpkg-deb -x ${FNAME} ${DNAME}
fakeroot dpkg-deb -e ${FNAME} ${DNAME}/DEBIAN
fakeroot chmod 0644 ${DNAME}/DEBIAN/md5sums
rm ${FNAME}
fakeroot dpkg-deb -b ${DNAME} ${FNAME}
rm -rf ${DNAME}

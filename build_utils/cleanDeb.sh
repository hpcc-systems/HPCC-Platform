#!/bin/bash
################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
rm ${FNAME}
fakeroot dpkg-deb -b ${DNAME} ${FNAME}
rm -rf ${DNAME}

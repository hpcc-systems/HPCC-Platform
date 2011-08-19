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

function strip_pattern () {
    PATTERN="$1"
    grep -v -E "${PATTERN}" ${2:--}
}


function alter_file () {
    BASE_FILE="$1"
    STRIP="$2"
    strip_pattern "${STRIP}" "${BASE_FILE}" > "${BASE_FILE}.$$.new"
    cat - >> "${BASE_FILE}.$$.new"
    # exchange new and old file, preserving owner/permissions
    mv "${BASE_FILE}" "${BASE_FILE}.$$.old"
    mv "${BASE_FILE}.$$.new" "${BASE_FILE}"
    chmod --reference="${BASE_FILE}.$$.old" "${BASE_FILE}"
    chown --reference="${BASE_FILE}.$$.old" "${BASE_FILE}"
    rm -f "${BASE_FILE}.$$.old"
}


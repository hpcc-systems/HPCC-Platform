################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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


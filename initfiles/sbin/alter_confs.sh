################################################################################
## Copyright © 2011 HPCC Systems.  All rights reserved.
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


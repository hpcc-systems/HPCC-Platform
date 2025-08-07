#!/bin/bash

run_eclcc() {
    echo "Running eclcc $@"
    rm -f a.out

    # Run eclcc and capture exit status immediately
    eclcc "$@" -platform=hthor
    local exit_status=$?
    if [ $exit_status -ne 0 ]; then
        echo "ERROR: eclcc compilation failed for '$@' with exit status $exit_status"
        exit 1
    fi

    if [ ! -x ./a.out ]; then
        echo "ERROR: Compiled binary './a.out' does not exist or is not executable."
        exit 1
    fi
}

rm -f value_types.tar
if ! tar --create -f value_types.tar value_types.ecl; then
    echo "ERROR: Failed to create tar file"
    exit 1
fi

# Compile directly from a tar file
run_eclcc value_types.tar/value_types.ecl

# Compile directly from a file checked into a github repository
run_eclcc --main ecl.regress.value_types@hpcc-systems/HPCC-Platform#master

# Clean up
rm -f value_types.tar a.out

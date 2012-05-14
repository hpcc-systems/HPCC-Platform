#!/usr/bin/env bash
###############################################################################
#  Copyright (C) 2011 HPCC Systems.
#
#  All rights reserved. This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Affero General Public License as
#  published by the Free Software Foundation, either version 3 of the
#  License, or (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Affero General Public License for more details.
#
#  You should have received a copy of the GNU Affero General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
##############################################################################
# Instructions:
#
# To create a golden standard, checkout master top and run:
#   ./regress.sh -t golden [other options]
#
# Then, checkout your branch, compile and run:
#   ./regress.sh -t my_branch -c golden [other options]
#
# Note that, if you don't install the package, you need to get the eclcc
# in your build directory. (option: -e $BUILDDIR/Debug/bin/eclcc)
##############################################################################

syntax="syntax: $0 [-t target_dir] [-c compare_dir] [-i include_dir] [-e eclcc] [-d diff_program]"

## Create Makefile (git doesn't like tabs)
echo "FILES=\$(shell echo *.ecl*)" > Makefile
echo "LOGS_=\$(FILES:%.ecl=\$(target_dir)/%.log)" >> Makefile
echo "LOGS=\$(LOGS_:%.eclxml=\$(target_dir)/%.log)" >> Makefile
echo >> Makefile
echo "all: \$(LOGS)" >> Makefile
echo >> Makefile
echo "\$(target_dir)/%.log: %.ecl" >> Makefile
echo -e "\t\$(eclcc) \$(flags) $^" >> Makefile
echo >> Makefile
echo "\$(target_dir)/%.log: %.eclxml" >> Makefile
echo -e "\t\$(eclcc) \$(flags) $^" >> Makefile

## Default arguments
target_dir=run_$$
compare_dir=
include_dir=
compare_only=0
eclcc=
diff=
np=`grep -c processor /proc/cpuinfo`

## Get cmd line options (overrite default args)
if [[ $1 = '' ]]; then
    echo
    echo $syntax
    echo " * target_dir automatically created with run_pid"
    echo " * compare_dir necessary for comparisons"
    echo " * include dir for special ECL headers"
    echo " * eclcc necessary for compilation, otherwise, only comparison will be made"
    echo " * diff_program must be able to handle directories"
    echo
    exit -1
fi
if [[ $* != '' ]]; then
    while getopts "t:c:i:e:d:" opt; do
        case $opt in
            t)
                target_dir=$OPTARG
                ;;
            c)
                compare_dir=$OPTARG
                ;;
            i)
                include_dir=$OPTARG
                ;;
            e)
                eclcc=$OPTARG
                ;;
            d)
                diff=$OPTARG
                ;;
            :)
                echo $syntax
                exit -1
                ;;
        esac
    done
fi

if [[ $eclcc != '' ]]; then
    ## Set flags
    default_flags="-P$target_dir -legacy -target=thorlcr -fforceGenerate -fdebugNlp=1 -fnoteRecordSizeInGraph -fregressionTest -b -m -S -shared -faddTimingToWorkunit=0 -fshowRecordCountInGraph"
    flags="$default_flags $include_dir -fshowMetaInGraph"

    ## Prepare target directory
    rm -rf $target_dir
    mkdir -p $target_dir

    ## Compile all regressions
    echo "* Compiling all regression tests"
    echo
    export eclcc flags include_dir target_dir
    time make -j $np > /dev/null
fi

## Compare to golden standard (ignore obvious differences)
if [[ $compare_dir ]]; then
    if [[ ! -d $target_dir ]]; then
        echo " ++ No target dir to compare"
        exit 1
    fi
    echo "* Comparing to $compare_dir"
    echo
    if [[ $diff != '' ]]; then
        $diff $compare_dir $target_dir 2> /dev/null
    else
        diff -I $compare_dir -I $target_dir \
             -I '\d* ms' \
             -I 'at \/.*\(\d*\)$' \
             -I '\d*s total time' \
             -I 'hash=\"[0-9a-f]*' \
             -q $compare_dir $target_dir
    fi
fi

# Confirmation
echo "* All Done"

#!/usr/bin/env bash
###############################################################################
#  HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
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

syntax="syntax: $0 [-t target_dir] [-c compare_dir] [-I include_dir ...] [-e eclcc] [-d diff_program] [-q query.ecl] [-l log_file] "

## Default arguments
target_dir=run_$$
compare_dir=-
include_dir=
compare_only=0
userflags=
eclcc=
diff=
query=
valgrind=
np=`getconf _NPROCESSORS_ONLN`
export ECLCC_ECLINCLUDE_PATH=

## Get cmd line options (overrite default args)
if [[ $1 = '' ]]; then
    echo
    echo $syntax
    echo " * target_dir automatically created with run_pid"
    echo " * compare_dir necessary for comparisons"
    echo " * include dir for special ECL headers (allows multiple paths)"
    echo " * eclcc necessary for compilation, otherwise, only comparison will be made"
    echo " * diff_program must be able to handle directories"
    echo
    echo " * -q can be used to run/rerun a single query"
    echo " * -l is used to generate a detailed log for debugging"
    echo " * -p changes the number of parallel compiles"
    echo " * -f allows you to add a debug option"
    echo " * -x allows you to pass through an arbitrary option"
    echo
    exit -1
fi
if [[ $* != '' ]]; then
    while getopts "t:c:I:e:d:f:q:l:p:x:vw" opt; do
        case $opt in
            t)
                target_dir=$OPTARG
                ;;
            c)
                compare_dir=$OPTARG
                ;;
            I)
                include_dir="$include_dir -I$OPTARG"
                ;;
            e)
                eclcc=$OPTARG
                ;;
            d)
                diff=$OPTARG
                ;;
            f)
                userflags="$userflags -f$OPTARG"
                ;;
            q)
                query="$OPTARG"
                ;;
            l)
                userflags="$userflags --logdetail 999 --logfile $OPTARG"
                ;;
            x)
                userflags="$userflags $OPTARG"
                ;;
            p)
                np="$OPTARG"
                ;;
            v)
                valgrind="valgrind --leak-check=full --suppressions=suppressions.txt "
                userflags="$userflags --leakcheck"
                eclcc="$valgrind $eclcc"
                compare_dir=
                ;;
            w)
                valgrind="valgrind --leak-check=full --suppressions=suppressions.txt --vgdb-error=0 "
                userflags="$userflags --leakcheck"
                valgrind="$valgrind --track-origins=yes "
                eclcc="$valgrind $eclcc"
                compare_dir=
                ;;
            :)
                echo $syntax
                exit -1
                ;;
        esac
    done
    shift $((OPTIND -1))
fi

#If the filename is provided without -q, default to compiling just that query
if [[ $* != '' ]]; then
    query="$*"
fi

if [[ $eclcc != '' ]]; then
    ## Set flags
    default_flags="-P$target_dir -legacy -platform=thorlcr -fforceGenerate -fregressionTest -b -S -shared -meta+"
    flags="$default_flags $include_dir -fshowMetaInGraph -fspanMultipleCpp- $userflags"

    ## Prepare target directory
    if [[ $query == '' ]]; then
        rm -rf $target_dir
    fi
    mkdir -p $target_dir

    ## Create Makefile (git doesn't like tabs)
    echo "#Auto generated make file" > Makefile
    echo "FLAGS=$flags" >> Makefile
    echo "ECLCC=$eclcc" >> Makefile
    echo "TARGET=$target_dir" >> Makefile
    echo "FILES=\$(shell ls *.ecl* | sort -R | tr '\n' ' ')" >> Makefile
    echo "LOGS0=\$(FILES:%.ecl=\$(TARGET)/%.ecl.log)" >> Makefile
    echo "LOGS1=\$(LOGS0:%.ecl.gz=\$(TARGET)/%.ecl.log)" >> Makefile
    echo "LOGS2=\$(LOGS1:%.eclxml.gz=\$(TARGET)/%.eclxml.log)" >> Makefile
    echo "LOGS=\$(LOGS2:%.eclxml=\$(TARGET)/%.eclxml.log)" >> Makefile
    echo >> Makefile
    echo "all: \$(LOGS)" >> Makefile
    echo >> Makefile
    echo "%.run: \$(TARGET)/%.log" >> Makefile
    echo -e "\t#do nothing" >> Makefile
    echo >> Makefile
    echo "\$(TARGET)/%.ecl.log: %.ecl" >> Makefile
    echo -e "\t\$(ECLCC) \$(FLAGS) $^" >> Makefile
    echo >> Makefile
    echo "\$(TARGET)/%.ecl.log: %.ecl.gz" >> Makefile
    echo -e "\t\$(ECLCC) \$(FLAGS) $^" >> Makefile
    echo >> Makefile
    echo "\$(TARGET)/%.eclxml.log: %.eclxml" >> Makefile
    echo -e "\t\$(ECLCC) \$(FLAGS) $^" >> Makefile
    echo >> Makefile
    echo "\$(TARGET)/%.eclxml.log: %.eclxml.gz" >> Makefile
    echo -e "\t\$(ECLCC) \$(FLAGS) $^" >> Makefile

    if [[ $query != '' ]]; then
        ## Compile all regressions
        echo "Compiling $query regression test"
        echo
        time make $query.run -B > /dev/null
    else
        ## Compile all regressions
        echo "* Compiling all regression tests"
        echo
        time make -k -j $np > /dev/null
    fi
fi

## Compare to golden standard (ignore obvious differences)
if [[ $compare_dir != '-' ]]; then
    if [[ ! -d $target_dir ]]; then
        echo " ++ No target dir to compare"
        exit 1
    fi
    echo "* Comparing to $compare_dir"
    echo
    if [[ $diff != '' ]]; then
        $diff $compare_dir $target_dir 2> /dev/null &
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

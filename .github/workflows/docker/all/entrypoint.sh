#!/bin/bash

source /hpcc-dev/hpccinstall/opt/HPCCSystems/sbin/hpcc_setenv
/hpcc-dev/hpccinstall/opt/HPCCSystems/etc/init.d/hpcc-init start

./ecl-test setup
./ecl-test run --pq 2 --excludeclass embedded,3rdparty,spray

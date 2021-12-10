#!/usr/bin/env bash

drop1p="--dropOkToSendPackets=100 --dropRequestReceivedPackets=100 --dropRequestToSendPackets=100 --dropRequestToSendMorePackets=100 --dropSendCompletedPackets=100 --dropSendStartPackets=100"
drop1d="--dropDataPackets=100"
drop1dp="$drop1d $drop1p"
drop2="--dropDataPackets=50 --dropOkToSendPackets=50 --dropRequestReceivedPackets=50 --dropRequestToSendPackets=50 --dropRequestToSendMorePackets=50 --dropSendCompletedPackets=50   --dropSendStartPackets=50"
drop5="--dropDataPackets=20 --dropOkToSendPackets=20 --dropRequestReceivedPackets=20 --dropRequestToSendPackets=20 --dropRequestToSendMorePackets=20 --dropSendCompletedPackets=20   --dropSendStartPackets=20"
drop50="--dropDataPackets=2 --dropOkToSendPackets=2 --dropRequestReceivedPackets=2 --dropRequestToSendPackets=2 --dropRequestToSendMorePackets=2 --dropSendCompletedPackets=2   --dropSendStartPackets=2"

sudocmd=
udpsimdir=""
if [ -n "$UDPSIMDIR" ]
then
  udpsimdir="$UDPSIMDIR"
fi

if [[ $* != '' ]]; then
    while getopts "p:s" opt; do
        case $opt in
            p)
                udpsimdir="$OPTARG/"
                ;;
            s)
                sudocmd="sudo"
                ;;
            ?)
                echo "syntax: $0"
                echo "   -p <path-to-udpsim>    Specify the path to udpsim"
                echo "   -s                     Run udpsim as sudo (so thread priorities can be set)"
                exit -1
                ;;
        esac
    done
    shift $((OPTIND -1))
fi

function doit()
{
    echo "$sudocmd ${udpsimdir}udpsim $1 2> $2.txt"
    $sudocmd ${udpsimdir}udpsim $1 2> $2.txt
}

sudo echo Synchronous transfers
doit "--udpAllowAsyncPermits=0 --udpResendTimeout=0 --numThreads=1 --packetsPerThread=1000000" sync1
doit "--udpAllowAsyncPermits=0 --udpResendTimeout=0 --numThreads=10 --packetsPerThread=100000" sync10

doit "--udpAllowAsyncPermits=0 --udpResendTimeout=0 --numThreads=1 --packetsPerThread=1000000 $drop1d" sync1d1
doit "--udpAllowAsyncPermits=0 --udpResendTimeout=0 --numThreads=1 --packetsPerThread=1000000 $drop1p" sync1p1
doit "--udpAllowAsyncPermits=0 --udpResendTimeout=0 --numThreads=1 --packetsPerThread=1000000 $drop1dp" sync1dp1

doit "--udpAllowAsyncPermits=0 --udpResendTimeout=0 --numThreads=10 --packetsPerThread=100000 $drop1d" sync10d1
doit "--udpAllowAsyncPermits=0 --udpResendTimeout=0 --numThreads=10 --packetsPerThread=100000 $drop1p" sync10p1
doit "--udpAllowAsyncPermits=0 --udpResendTimeout=0 --numThreads=10 --packetsPerThread=100000 $drop1dp" sync10dp1

echo
echo Asynchronous transfers
doit "--udpAllowAsyncPermits=1 --udpResendTimeout=5 --numThreads=1 --packetsPerThread=1000000" async1
doit "--udpAllowAsyncPermits=1 --udpResendTimeout=5 --numThreads=10 --packetsPerThread=100000" async10

doit "--udpAllowAsyncPermits=1 --udpResendTimeout=5 --numThreads=1 --packetsPerThread=1000000 $drop1d" async1d1
doit "--udpAllowAsyncPermits=1 --udpResendTimeout=5 --numThreads=1 --packetsPerThread=1000000 $drop1p" async1p1
doit "--udpAllowAsyncPermits=1 --udpResendTimeout=5 --numThreads=1 --packetsPerThread=1000000 $drop1dp" async1dp1

doit "--udpAllowAsyncPermits=1 --udpResendTimeout=5 --numThreads=10 --packetsPerThread=100000 $drop1d" async10d1
doit "--udpAllowAsyncPermits=1 --udpResendTimeout=5 --numThreads=10 --packetsPerThread=100000 $drop1p" async10p1
doit "--udpAllowAsyncPermits=1 --udpResendTimeout=5 --numThreads=10 --packetsPerThread=100000 $drop1dp" async10dp1

doit "--udpAllowAsyncPermits=1 --udpResendTimeout=20 --numThreads=10 --packetsPerThread=100000 $drop1d" async10d1t20
doit "--udpAllowAsyncPermits=1 --udpResendTimeout=50 --numThreads=10 --packetsPerThread=100000 $drop1d" async10d1t50

#sudo RelWithDebInfo/bin/udpsim --udpAllowAsyncPermits=0 --udpResendTimeout=0 --numThreads=1 --packetsPerThread=1000000 $drop2 2> sync1d2.txt
#sudo RelWithDebInfo/bin/udpsim --udpAllowAsyncPermits=0 --udpResendTimeout=0 --numThreads=1 --packetsPerThread=1000000 $drop5 2> sync1d5.txt
#sudo RelWithDebInfo/bin/udpsim --udpAllowAsyncPermits=0 --udpResendTimeout=0 --numThreads=1 --packetsPerThread=1000000 $drop50 2> sync1d50.txt

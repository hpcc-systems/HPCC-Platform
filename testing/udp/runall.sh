#!/usr/bin/env bash

set -eu

drop1p="--dropOkToSendPackets=100 --dropRequestReceivedPackets=100 --dropRequestToSendPackets=100 --dropRequestToSendMorePackets=100 --dropSendCompletedPackets=100 --dropSendStartPackets=100"
drop1d="--dropDataPackets=1"
drop1dp="$drop1d $drop1p"
drop2="--dropDataPackets=1 --dropDataPacketsPercent=2 --dropOkToSendPackets=50 --dropRequestReceivedPackets=50 --dropRequestToSendPackets=50 --dropRequestToSendMorePackets=50 --dropSendCompletedPackets=50   --dropSendStartPackets=50"
drop5="--dropDataPackets=1 --dropDataPacketsPercent=5 --dropOkToSendPackets=20 --dropRequestReceivedPackets=20 --dropRequestToSendPackets=20 --dropRequestToSendMorePackets=20 --dropSendCompletedPackets=20   --dropSendStartPackets=20"
drop50="--dropDataPackets=1 --dropDataPacketsPercent=50 --dropOkToSendPackets=2 --dropRequestReceivedPackets=2 --dropRequestToSendPackets=2 --dropRequestToSendMorePackets=2 --dropSendCompletedPackets=2   --dropSendStartPackets=2"

legacy=""
sync="--udpAllowAsyncPermits=0 --udpResendTimeout=0"
async="--udpAllowAsyncPermits=1 --udpResendTimeout=1"
synclegacy="$sync --udpMaxPendingPermits=1"
sync10p="$sync --udpMaxClientPercent=100"
sync20p="$sync --udpMaxClientPercent=200"
sync30p="$sync --udpMaxClientPercent=300"
sync40p="$sync --udpMaxClientPercent=400"
sync50p="$sync --udpMaxClientPercent=500"
sync60p="$sync --udpMaxClientPercent=600"
sync80p="$sync --udpMaxClientPercent=800"
sync100p="$sync --udpMaxClientPercent=1000"
syncm1="$sync --udpMinSlotsPerSender=1"
syncm2="$sync --udpMinSlotsPerSender=2"
syncm5="$sync --udpMinSlotsPerSender=5"
syncm10="$sync --udpMinSlotsPerSender=10"
syncm20="$sync --udpMinSlotsPerSender=20"
syncm50="$sync --udpMinSlotsPerSender=50"
syncm100="$sync --udpMinSlotsPerSender=100"
syncm200="$sync --udpMinSlotsPerSender=200"

asynclegacy="$async --udpMaxClientPercent=100 --udpMaxPendingPermits=1"
async10p="$async --udpMaxClientPercent=100"
async20p="$async --udpMaxClientPercent=200"
async30p="$async --udpMaxClientPercent=300"
async40p="$async --udpMaxClientPercent=400"
async50p="$async --udpMaxClientPercent=500"
async60p="$async --udpMaxClientPercent=600"
async80p="$async --udpMaxClientPercent=800"
async100p="$async --udpMaxClientPercent=1000"

synclegacyprio="$synclegacy --udpAdjustThreadPriorities=1"
synclegacynoprio="$synclegacy --udpAdjustThreadPriorities=0"
syncprio="$sync --udpAdjustThreadPriorities=1"
syncnoprio="$sync --udpAdjustThreadPriorities=0"

sudocmd=
udpsimdir=""
numrows=1000000
corerange="16-31 16-23 16-19 16-17"
receiverange="40 100 500"
threadrange=$(seq 10)
workrange="0"
iters=5
options=""
testqueues=""
testdropped=""
verbose=""
optextra=""

if [ -n "${UDPSIMDIR:-}" ]
then
  udpsimdir="$UDPSIMDIR"
fi

if [[ $* != '' ]]; then
    while getopts "c:di:n:o:p:qr:st:vw:x:" opt; do
        case $opt in
            c)
                corerange="$OPTARG"
                ;;
            d)
                testdropped="1"
                ;;
            i)
                iters=$OPTARG
                ;;
            n)
                numrows="$OPTARG"
                ;;
            o)
                options="$OPTARG"
                ;;
            p)
                udpsimdir="$OPTARG/"
                ;;
            q)
                testqueues="1"
                ;;
            r)
                receiverange="$OPTARG"
                ;;
            s)
                sudocmd="sudo"
                ;;
            t)
                threadrange="$OPTARG"
                ;;
            v)
                verbose="1"
                ;;
            w)
                workrange="$OPTARG"
                ;;
            x)
                optextra="$optextra $OPTARG"
                ;;
            ?)
                echo "syntax: $0"
                echo "   -c \"<core list>\"     Allow list of receiver sizes to be specified"
                echo "   -d                     test dropped packets"
                echo "   -i <num-iters>         How many times to run each test"
                echo "   -o legacy|sync|async   List of options to run combinations"
                echo "   -p <path-to-udpsim>    Specify the path to udpsim"
                echo "   -q                     test queue versions "
                echo "   -r \"<receiver size list>\"   Allow list of receiver sizes to be specified"
                echo "   -s                     Run udpsim as sudo (so thread priorities can be set)"
                echo "   -t \"<thread list>\"   Allow list of sending threads to be specified"
                echo "   -v                     echo test before running"
                echo "   -w \"<work list>\"     Allow list of sending threads to be specified"
                echo "   -x option              Pass option through as an argument to udpsim"
                exit -1
                ;;
        esac
    done
    shift $((OPTIND -1))
fi

function doit()
{
    if [ -n "$verbose" ]
    then
        echo "$sudocmd ${udpsimdir}udpsim $1 $optextra 2> udp.$2.txt"
    fi
    rm -f "udp.$2.time"
    rm -f "udp.$2.txt"
    echo "$sudocmd ${udpsimdir}udpsim $1 $optextra 2> udp.$2.txt" > "udp.$2.txt"
    for i in $(seq $iters)
    do
        $sudocmd ${udpsimdir}udpsim $1 $optextra >> "udp.$2.time" 2>> "udp.$2.txt"
    done
    # Extract the time taken from the logs, sort numerically, then output them on a single line so it is easy to see the median.
    grep -o [0-9]*ms "udp.$2.time" | sort -n | (readarray -t ARRAY; IFS=' '; echo "Time: ${ARRAY[*]} ${3:-}")
}

function doall()
{
    oldsudo=$sudocmd
    version=$1
    extra="${2:-}"
    options="${!version}"
    $sudocmd echo "label d1 d2 d3 d4 d5 threads slots cores version work"
    for cores in $corerange
    do
      sudocmd="$oldsudo taskset -c $cores"
      for work in $workrange
      do
        for slots in $receiverange
        do
          for threads in $threadrange
          do
            let perthread=$numrows/$threads
            doit "$options --numThreads=$threads --packetsPerThread=$perthread --udpTraceTimeouts=0 --numReceiveSlots=$slots --minWork=$work $extra" "${version}${threads}_${slots}_${cores}" "$threads $slots $cores $version $work"
          done
        done
      done
    done
    sudocmd="$oldsudo"
}

for option in $options
do
  doall $option
done


if [ -n "$testqueues" ]
then
    $sudocmd echo With queues
    doit "$sync --numThreads=1 --packetsPerThread=1000 --useQueue=1" syncq1
    $z
    doit "$sync --numThreads=10 --packetsPerThread=1000000 --useQueue=1" syncq10
    doit "$async --numThreads=1 --packetsPerThread=1000000 --useQueue=1" asyncq1
    doit "$async --numThreads=10 --packetsPerThread=1000000 --useQueue=1" asyncq10

    # Test delay/jitter - need to use a much lower number of packets because the delay of 1ms is approx 1000x longer than you would expect
    doit "$sync --numThreads=10 --packetsPerThread=10000 --useQueue=1 --udpTestSocketJitter=1 --udpTestSocketDelay=1" syncq10j
    doit "$async --numThreads=10 --packetsPerThread=10000 --useQueue=1 --udpTestSocketJitter=1 --udpTestSocketDelay=1" asyncq10j
fi

if [ -n "$testdropped" ]
then
    $sudocmd echo
    $sudocmd echo Synchronous transfers
    # Single sender, normal, and how does it cope with dropped packets.  Disable timeout tracing because it signficantly
    # increases the time when packets are being dropped
    doit "$sync --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0" sync1
    doit "$sync --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d" sync1d1
    doit "$sync --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1p" sync1p1
    doit "$sync --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1dp" sync1dp1

    # How does it scale with the number of senders
    doit "$sync --numThreads=2 --packetsPerThread=500000" sync2
    doit "$sync --numThreads=4 --packetsPerThread=250000" sync4
    doit "$sync --numThreads=6 --packetsPerThread=166666" sync6

    # How does it scale with the number of dropped data packets?
    doit "$sync --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d --dropDataPacketsPercent=1" sync1d1_1
    doit "$sync --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d --dropDataPacketsPercent=2" sync1d1_2
    doit "$sync --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d --dropDataPacketsPercent=5" sync1d1_5
    doit "$sync --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d --dropDataPacketsPercent=10" sync1d1_10
    doit "$sync --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d --dropDataPacketsPercent=20" sync1d1_20
    doit "$sync --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d --dropDataPacketsPercent=50" sync1d1_50
    doit "$sync --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d --dropDataPacketsPercent=50" sync1d1_100

    # How does it scale with the size of the receive queue?
    doit "$sync --numThreads=5 --packetsPerThread=200000 --numReceiveSlots=20" sync5r20
    doit "$sync --numThreads=5 --packetsPerThread=200000 --numReceiveSlots=50" sync5r50
    doit "$sync --numThreads=5 --packetsPerThread=200000 --numReceiveSlots=100" sync5r100
    doit "$sync --numThreads=5 --packetsPerThread=200000 --numReceiveSlots=200" sync5r200
    doit "$sync --numThreads=5 --packetsPerThread=200000 --numReceiveSlots=500" sync5r500
    doit "$sync --numThreads=5 --packetsPerThread=200000 --numReceiveSlots=1000" sync5r1000

    # Performance with 10 senders - normal, only a single permit, and when packets are being dropped
    doit "$sync --numThreads=10 --packetsPerThread=100000" sync10
    doit "$sync --numThreads=10 --packetsPerThread=100000 --udpMaxPendingPermits=1" sync10x1
    doit "$sync --numThreads=10 --packetsPerThread=100000 --udpTraceTimeouts=0 $drop1d" sync10d1
    doit "$sync --numThreads=10 --packetsPerThread=100000 --udpTraceTimeouts=0 $drop1p" sync10p1
    doit "$sync --numThreads=10 --packetsPerThread=100000 --udpTraceTimeouts=0 $drop1dp" sync10dp1

    # Dropping different percentages of the data and flow packets  (reduced number of rows)
    doit "$sync --numThreads=1 --packetsPerThread=100000 $drop2" sync1d2
    doit "$sync --numThreads=1 --packetsPerThread=100000 $drop5" sync1d5
    doit "$sync --numThreads=1 --packetsPerThread=100000 $drop50" sync1d50

    $sudocmd echo
    $sudocmd echo Asynchronous transfers
    # Same test cases as above
    doit "$async --numThreads=1 --packetsPerThread=1000000" async1
    doit "$async --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d" async1d1
    doit "$async --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1p" async1p1
    doit "$async --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1dp" async1dp1

    # How does it scale with the number of senders
    doit "$async --numThreads=2 --packetsPerThread=500000" async2
    doit "$async --numThreads=4 --packetsPerThread=250000" async4
    doit "$async --numThreads=6 --packetsPerThread=166666" async6

    # How does it scale with the number of dropped data packets?
    doit "$async --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d --dropDataPacketsPercent=1" async1d1_1
    doit "$async --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d --dropDataPacketsPercent=2" async1d1_2
    doit "$async --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d --dropDataPacketsPercent=5" async1d1_5
    doit "$async --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d --dropDataPacketsPercent=10" async1d1_10
    doit "$async --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d --dropDataPacketsPercent=20" async1d1_20
    doit "$async --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d --dropDataPacketsPercent=50" async1d1_50
    doit "$async --numThreads=1 --packetsPerThread=1000000 --udpTraceTimeouts=0 $drop1d --dropDataPacketsPercent=50" async1d1_100

    # How does it scale with the size of the receive queue?
    doit "$async --numThreads=5 --packetsPerThread=200000 --numReceiveSlots=20" async5r20
    doit "$async --numThreads=5 --packetsPerThread=200000 --numReceiveSlots=50" async5r50
    doit "$async --numThreads=5 --packetsPerThread=200000 --numReceiveSlots=100" async5r100
    doit "$async --numThreads=5 --packetsPerThread=200000 --numReceiveSlots=200" async5r200
    doit "$async --numThreads=5 --packetsPerThread=200000 --numReceiveSlots=500" async5r500
    doit "$async --numThreads=5 --packetsPerThread=200000 --numReceiveSlots=1000" async5r1000

    # Performance with 10 senders - normal, only a single permit, and when packets are being dropped
    doit "$async --numThreads=10 --packetsPerThread=100000" async10
    doit "$async --numThreads=10 --packetsPerThread=100000 --udpTraceTimeouts=0 $drop1d" async10d1
    doit "$async --numThreads=10 --packetsPerThread=100000 --udpTraceTimeouts=0 $drop1p" async10p1

    # What performance difference does the resend timeout make?
    doit "$async --udpResendTimeout=1 --numThreads=10 --packetsPerThread=100000 --udpTraceTimeouts=0 $drop1dp" async10dp1t1
    doit "$async --udpResendTimeout=5 --numThreads=10 --packetsPerThread=100000 --udpTraceTimeouts=0 $drop1dp" async10dp1
    doit "$async --udpResendTimeout=20 --numThreads=10 --packetsPerThread=100000 --udpTraceTimeouts=0 $drop1d" async10d1t20
    doit "$async --udpResendTimeout=50 --numThreads=10 --packetsPerThread=100000 --udpTraceTimeouts=0 $drop1d" async10d1t50
fi

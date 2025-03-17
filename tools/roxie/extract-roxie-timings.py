#!/usr/bin/env python3

'''
/*#############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems(R).

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################ */
'''

import csv
import sys
import re
import argparse
import datetime

def calculateDerivedStats(curRow):

    timeElapsed = float(curRow.get("elapsed", 0.0))

    numBranchHits = float(curRow.get("NumNodeCacheHits", 0.0))
    numBranchAdds = float(curRow.get("NumNodeCacheAdds", 0.0))
    numBranchFetches = float(curRow.get("NumNodeDiskFetches", 0.0))
    timeBranchFetches = float(curRow.get("TimeNodeFetch", 0.0))
    timeBranchRead = float(curRow.get("TimeNodeRead", 0.0))
    timeBranchLoad = float(curRow.get("TimeNodeLoad", 0.0))
    timeBranchDecompress = timeBranchLoad - timeBranchRead
    avgTimeBranchDecompress = 0 if numBranchAdds == 0 else timeBranchDecompress / numBranchAdds

    if numBranchHits + numBranchAdds:
        curRow["%BranchMiss"] = 100*numBranchAdds/(numBranchAdds+numBranchHits)

    numLeafHits = float(curRow.get("NumLeafCacheHits", 0.0))
    numLeafAdds = float(curRow.get("NumLeafCacheAdds", 0.0))
    numLeafFetches = float(curRow.get("NumLeafDiskFetches", 0.0))
    timeLeafFetches = float(curRow.get("TimeLeafFetch", 0.0))
    timeLeafRead = float(curRow.get("TimeLeafRead", 0.0))
    timeLeafLoad = float(curRow.get("TimeLeafLoad", 0.0))
    timeLeafDecompress = timeLeafLoad - timeLeafRead
    avgTimeLeafDecompress = 0 if numLeafAdds == 0 else timeLeafDecompress / numLeafAdds

    timeLocalExecute = float(curRow.get("TimeLocalExecute", 0.0))
    timeAgentWait = float(curRow.get("TimeAgentWait", 0.0))
    timeAgentQueue = float(curRow.get("TimeAgentQueue", 0.0))
    timeAgentProcess = float(curRow.get("TimeAgentProcess", 0.0))
    timeSoapcall = float(curRow.get("TimeSoapcall", 0.0))
    sizeAgentReply = float(curRow.get("SizeAgentReply", 0.0))
    sizeAgentRequests = float(curRow.get("SizeAgentRequests", 0.0))

    agentRequestEstimate = numLeafHits + numLeafAdds                                  # This could be completely wrong, but better than nothing
    numAgentRequests = float(curRow.get("NumAgentRequests", agentRequestEstimate))    # 9.8.x only

    timeLocalCpu = timeLocalExecute - timeAgentWait - timeSoapcall
    timeRemoteCpu = timeAgentProcess - timeLeafRead - timeBranchRead
    workerCpuLoad = timeRemoteCpu / timeAgentProcess if timeAgentProcess else 0

    if numLeafHits + numLeafAdds:
        curRow["%LeafMiss"] = 100*numLeafAdds/(numLeafAdds+numLeafHits)

    if numBranchAdds:
        curRow["%BranchFetch"] = 100*(numBranchFetches)/(numBranchAdds)
        curRow["TimeBranchDecompress"] = timeBranchDecompress
        curRow["AvgTimeBranchDecompress"] = avgTimeBranchDecompress

    if numLeafAdds:
        curRow["%LeafFetch"] = 100*(numLeafFetches)/(numLeafAdds)
        curRow["TimeLeafDecompress"] = timeLeafDecompress
        curRow["AvgTimeLeafDecompress"] = avgTimeLeafDecompress

    if numBranchFetches:
        curRow["AvgTimeBranchFetch"] = timeBranchFetches/(numBranchFetches)

    if (numLeafAdds + numLeafHits):
        numIndexRowsRead = float(curRow.get("NumIndexRowsRead", 0.0))
        curRow["Rows/Leaf"] = numIndexRowsRead / (numLeafAdds + numLeafHits)

    if numLeafFetches:
        curRow["AvgTimeLeafFetch"] = timeLeafFetches/(numLeafFetches)

    curRow["WorkerCpuLoad"] = workerCpuLoad
    curRow["TimeLocalCpu"] = timeLocalCpu
    curRow["TimeRemoteCpu"] = timeRemoteCpu

    if numAgentRequests:
        curRow["avgTimeAgentProcess"] = timeAgentProcess / numAgentRequests

    # Generate a summary analyis for the transaction(s)
    normalSummary = ''
    unusualSummary = ''
    notes = ''
    if numBranchFetches or numLeafFetches:
        avgDiskReadDelay = (timeLeafFetches+timeBranchFetches)/(numLeafFetches+numBranchFetches)
        if avgDiskReadDelay < 150:
            normalSummary += ",Disk Fetch"
        elif avgDiskReadDelay < 2000:
            unusualSummary += ",Disk Fetch slow for NVME"
        else:
            unusualSummary += ",Disk Fetch slow for remote storage"

    if numBranchAdds < (numBranchAdds + numBranchHits) // 100:
        normalSummary += ",Branch hits"
    else:
        normalSummary += ",Branch hits"

    if numLeafAdds < (numLeafAdds + numLeafHits) * 50 // 100:
        normalSummary += ",Leaf hits"
    else:
        normalSummary += ",Leaf hits"

    if timeSoapcall > 0:
        if timeSoapcall * 2 > timeLocalExecute:
            notes += ", Most of time in soapcall"

    if timeAgentWait == 0:
        notes += ", Only executes on the server"
    else:
        if timeAgentWait * 2 > timeLocalExecute:
            if timeAgentWait * 10 > timeLocalExecute * 8:
                notes += ", Agent bound"
            else:
                notes += ", Most of time in agent"
        else:
            if (timeAgentWait + timeSoapcall) < (timeLocalExecute / 5):
                unusualSummary += f",Unexplained Server Time [{100 * (timeLocalExecute - timeAgentWait - timeSoapcall) / timeLocalExecute:.2f}%]"

    if sizeAgentReply > 1000000:
        unusualSummary += ",Size Agent reply"

    if timeLocalExecute * 10 < timeElapsed * 11:
        notes += f", Single threaded [{100 * timeLocalExecute / timeElapsed:.2f}%]"

    if timeAgentProcess:
        if (timeAgentQueue > timeAgentProcess):
            unusualSummary += ",Agent Queue backlog"

        if timeAgentWait > (timeAgentQueue + timeAgentProcess) * 2:
            unusualSummary += ",Agent send backlog"

        if timeBranchDecompress + timeLeafDecompress > timeAgentProcess / 4:
            unusualSummary += f",Decompress time [{100.0 * (timeBranchDecompress + timeLeafDecompress) / timeElapsed:.2f}%]"

    if timeLocalCpu and timeRemoteCpu:
        notes += f",remote cpu {100 * timeRemoteCpu / (timeLocalCpu + timeRemoteCpu):.0f}%"

    numAckRetries = float(curRow.get("NumAckRetries", 0.0))
    resentPackets = float(curRow.get("resentPackets", 0.0))

    if resentPackets:
        unusualSummary += ",Resent packets"
    elif numAckRetries:
        unusualSummary += ",Ack retries"

    curRow["unusual"] =  '"' + unusualSummary[1:] + '"'
    curRow["normal"] = '"' + normalSummary[1:] + '"'
    curRow["notes"] = '"' + notes[1:] + '"'

def calculateSummaryStats(curRow, numCpus, numRows):

    timeLocalCpu = float(curRow.get("TimeLocalCpu", 0.0))
    timeRemoteCpu = float(curRow.get("TimeRemoteCpu", 0.0))
    timeTotalCpu = timeLocalCpu + timeRemoteCpu
    workerCpuLoad = float(curRow.get("WorkerCpuLoad", 0.0))

    timeQueryResponseSeconds = float(curRow.get("elapsed", 0.0)) / 1000
    avgTimeQueryResponseSeconds = timeQueryResponseSeconds / numRows if numRows else 0

    perCpuTransactionsPerSecond = 1000 * numRows / timeTotalCpu if timeTotalCpu else 0
    maxTransactionsPerSecond = numCpus * perCpuTransactionsPerSecond
    maxWorkerThreads = numCpus / workerCpuLoad if workerCpuLoad else 0
    maxFarmers = maxTransactionsPerSecond * avgTimeQueryResponseSeconds

    curRow["perCpuTransactionsPerSecond"] = perCpuTransactionsPerSecond  # recorded but not reported
    curRow["MaxTransactionsPerSecond"] = maxTransactionsPerSecond
    curRow["MaxWorkerThreads"] = maxWorkerThreads
    curRow["MaxFarmers"] = maxFarmers

    #Expected cpu load for 10 transactions per second per node
    if perCpuTransactionsPerSecond:
        curRow["CpuLoad@10q/s"] = 10 / perCpuTransactionsPerSecond

def printRow(curRow):

    print(f'{curRow["_id_"]}', end='')
    for statName in allStats:
        if statName in curRow:
            value = curRow[statName]
            print(f',{value}', end='')
        else:
            print(f',', end='')

    print()


if __name__ == "__main__":
    allStats = dict(time=1, elapsed=1)
    allServices = dict()

    minTimeStamp = ''
    maxTimeStamp = ''
    minElapsed = 0

    completePattern = re.compile("COMPLETE: ([^ ]*)")
    elapsedPattern = re.compile("complete in ([0-9]*) ms")
    idPattern = re.compile(r"(\{[^}]*\})")
    yearMonthDayPattern = re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}")
    hourMinuteSecondPattern = re.compile("^([0-9]*)h([0-9]*)m([0-9.]*)s$")
    minuteSecondPattern = re.compile("^([0-9]*)m([0-9.]*)s$")
    centiles = [ 0, 50, 75, 95, 97, 100 ]

    parser=argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument("--all", "-a", help="Combine all services into a single result", action='store_true')
    parser.add_argument("--commentary", help="Add commentary to the end of the output", action='store_true')
    parser.add_argument("--cpu", "-c", type=int, default=8, help="Number of CPUs to use (default: 8)")
    parser.add_argument("--ignorecase", "-i", help="Use case-insensitive query names", action='store_true')
    parser.add_argument("--nosummary", "-n", help="Avoid including a summary", action='store_true')
    parser.add_argument("--summaryonly", "-s", help="Only generate a summary", action='store_true')
    args = parser.parse_args()
    combineServices = args.all
    suppressDetails = args.summaryonly
    reportSummary = not args.nosummary or args.summaryonly
    ignoreQueryCase = args.ignorecase
    cpus = args.cpu

    csv.field_size_limit(0x100000)
    with open(args.filename, encoding='latin1') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=' ')
        line_count = 0
        for row in csv_reader:
            numCols = len(row);
            #Protect against output which comes directly from log analytics.
            if numCols == 0:
                continue

            rowText = row[numCols-1]

            completeMatch = completePattern.search(rowText)
            if completeMatch:
                curRow = dict();
                mapping = rowText.split();

                serviceName = completeMatch.group(1)
                if ignoreQueryCase:
                    serviceName = serviceName.lower()

                idMatch = idPattern.search(mapping[0])
                if idMatch:
                    if combineServices:
                        curRow["_id_"] = serviceName + ':' + idMatch.group(1)
                    else:
                        curRow["_id_"] = idMatch.group(1)
                else:
                    curRow["_id_"] = mapping[0].replace(",","!");

                elapsedMatch = elapsedPattern.search(rowText)
                elapsed = int(elapsedMatch.group(1)) if elapsedMatch else 0
                curRow["elapsed"] = elapsed

                #MORE: Unimplemented - allow timestamp filtering
                timestamp = ''
                for i in range(len(row)):
                    if yearMonthDayPattern.match(row[i]):
                        timestamp = row[i] + ' ' + row[i+1]
                        curRow["time"] = row[i+1]
                        break

                if minTimeStamp == '' or timestamp < minTimeStamp:
                    minTimeStamp = timestamp
                    minElapsed = elapsed

                if maxTimeStamp == '' or timestamp > maxTimeStamp:
                    maxTimeStamp = timestamp

                nesting = list()
                prefix = ''
                suppress = 0
                for cur in mapping:
                    if "=" in cur:
                        equals = cur.index('=')
                        name = prefix + cur[0:equals]
                        value = cur[equals+1:]
                        if value == '{':
                            nesting.append(prefix)
                            prefix += name + '.'
                        elif value[0] == '[':
                            suppress += 1
                            continue;
                        elif value[-1] == ']':
                            suppress -= 1
                            continue;
                        elif name in ("priority", "WhenFirstRow","NumAllocations"):
                            continue
                        else:
                            if suppress > 0:
                                continue
                            allStats[name] = 1
                            castValue = -1
                            #Remove any trailing comma that should not be present
                            if value[-1] == ',':
                                value = value[0:-1]

                            #Apply  any scaling to the values
                            matchHMS = hourMinuteSecondPattern.match(value)
                            matchMS = minuteSecondPattern.match(value)
                            if matchHMS:
                                castValue = (float(matchHMS.group(1)) * 24 * 60 + float(matchHMS.group(2)) * 60 + float(matchHMS.group(3))) * 1000
                            elif matchMS:
                                castValue = (float(matchMS.group(1)) * 60 + float(matchMS.group(2))) * 1000
                            elif value.endswith("ns"):
                                castValue = float(value[0:-2])/1000000
                            elif value.endswith("us"):
                                castValue = float(value[0:-2])/1000
                            elif value.endswith("ms"):
                                castValue = float(value[0:-2])
                            elif value.endswith("s"):
                                castValue = float(value[0:-1])*1000
                            elif value.endswith("GB") or value.endswith("Gb"):
                                castValue = float(value[0:-2])*1000000
                            elif value.endswith("MB") or value.endswith("Mb"):
                                castValue = float(value[0:-2])*1000000
                            elif value.endswith("KB") or value.endswith("Kb"):
                                castValue = float(value[0:-2])*1000
                            elif value.endswith("B") or value.endswith("b"):
                                castValue = float(value[0:-1])
                            else:
                                try:
                                    castValue = int(value)
                                except ValueError:
                                    try:
                                        castValue = float(value)
                                    except ValueError:
                                        castValue = -1

                            if castValue != -1:
                                #Some values occur more than once - when reported separately for server and worker.
                                if name in curRow:
                                    curRow[name] += castValue
                                else:
                                    curRow[name] = castValue
                            else:
                                curRow[name] = value
                    elif '}' == cur[0]:
                        prefix = nesting.pop()

                if combineServices:
                    serviceName = 'all'
                if not serviceName in allServices:
                    allServices[serviceName] = list()
                allServices[serviceName].append(curRow)
                line_count += 1

    allStats[' '] = 1
    allStats["%BranchMiss"]=1
    allStats["%LeafMiss"] = 1
    allStats["%BranchFetch"] = 1
    allStats["%LeafFetch"] = 1
    allStats["AvgTimeBranchFetch"] = 1
    allStats["AvgTimeLeafFetch"] = 1
    allStats["TimeBranchDecompress"] = 1
    allStats["AvgTimeBranchDecompress"] = 1
    allStats["TimeLeafDecompress"] = 1
    allStats["AvgTimeLeafDecompress"] = 1
    allStats["Rows/Leaf"] = 1
    allStats["WorkerCpuLoad"] = 1
    allStats["TimeLocalCpu"] = 1
    allStats["TimeRemoteCpu"] = 1
    allStats["avgTimeAgentProcess"] = 1   # 9.8 only
    allStats['  '] = 1
    allStats['cpus='+str(cpus)] = 1
    allStats["MaxTransactionsPerSecond"] = 1
    allStats["MaxWorkerThreads"] = 1
    allStats["MaxFarmers"] = 1
    allStats["CpuLoad@10q/s"] = 1
    allStats["unusual"] = 1
    allStats["normal"] = 1
    allStats["notes"] = 1

    elapsed = 0
    try:
        minTime = datetime.datetime.strptime(minTimeStamp, '%Y-%m-%d %H:%M:%S.%f')
        maxTime = datetime.datetime.strptime(maxTimeStamp, '%Y-%m-%d %H:%M:%S.%f')
        elapsed = (maxTime - minTime).seconds + minElapsed/1000
        print(f"Time range: ['{minTimeStamp}'..'{maxTimeStamp}'] = {elapsed}s")

    except:
        pass

    # Create a string containing all the stats that were found in the file.
    headings =  'id'
    for statName in allStats:
        headings = headings + ',' + statName

    globalTotalRow = dict(_id_="summary")
    numGlobalRows = 0
    for service in allServices:
        allRows = allServices[service]
        numGlobalRows += len(allRows)

        # Calculate some derived statistics.
        for curRow in allRows:
            calculateDerivedStats(curRow)
            calculateSummaryStats(curRow, cpus, 1)

        # MORE: Min and max for the derived statistics are not correct

        if not combineServices:
            print(f"-----------{service} {len(allRows)} ------------")

        print(headings)

        if not suppressDetails:
            for curRow in allRows:
                printRow(curRow)

        print()

        if reportSummary:

            # Calculate the total for all rows, together with the derived statistics
            totalRow = dict(_id_="totals")
            for curRow in allRows:
                for name in curRow:
                    if name != '_id_':
                        value = curRow[name]
                        if type(value) != str:
                            if name in totalRow:
                                totalRow[name] += value
                            else:
                                totalRow[name] = value
                            globalTotalRow[name] = globalTotalRow.get(name, 0) + value

            calculateDerivedStats(totalRow)
            calculateSummaryStats(totalRow, cpus, len(allRows))

            # Average for all queries - should possibly also report average when stats are actually supplied
            numRows = len(allRows)
            avgRow = dict(_id_="avg", totalRow="avg")
            for statName in allStats:
                if statName in totalRow and type(totalRow[statName]) != str:
                    avgRow[statName] = float(totalRow[statName]) / numRows
            calculateDerivedStats(avgRow)

            # Now calculate the field values for each of the centiles that are requested
            centileRows = dict()
            for centile in centiles:
                centileRows[centile] = dict(_id_ = str(centile)+"%")

            for statName in allStats:
                # Useful way to sort all None values to the start and avoid errors comparing None with a field value
                def sortFunc(cur):
                    x = cur.get(statName)
                    return (not x is None, x)

                allRows.sort(key = sortFunc)
                for centile in centiles:
                    index = int((len(allRows)-1) * float(centile) / 100.0)
                    value = allRows[index].get(statName)
                    if not value is None:
                        centileRows[centile][statName] = value

            if not suppressDetails:
                print(headings)

            printRow(totalRow)
            printRow(avgRow)
            for centile in centiles:
                printRow(centileRows[centile])

            print()

    #These stats are only really revelant if it is including all the transactions from all services
    if reportSummary and elapsed and numGlobalRows:
        calculateDerivedStats(globalTotalRow)
        calculateSummaryStats(globalTotalRow, cpus, numGlobalRows)

        perCpuTransactionsPerSecond = globalTotalRow["perCpuTransactionsPerSecond"]
        totalDiskReads = (globalTotalRow.get("NumNodeDiskFetches", 0) + globalTotalRow.get("NumLeafDiskFetches", 0))
        actualTransationsPerSecond = numGlobalRows / elapsed
        expectedCpuLoad = actualTransationsPerSecond / perCpuTransactionsPerSecond if perCpuTransactionsPerSecond else 0
        iops = totalDiskReads / elapsed
        throughput = iops * 8192
        printRow(globalTotalRow)
        print()
        print(f"Transactions {numGlobalRows}q {elapsed}s: Throughput={actualTransationsPerSecond:.3f}q/s Time={1/actualTransationsPerSecond:.3f}s/q")
        print(f"ExpectedCpuLoad={expectedCpuLoad:.3f} iops={iops:.3f}/s DiskThroughput={throughput/1000000:.3f}MB/s")

        commentary = '''
"How can the output of this script be useful?  Here are some suggestions:"
""
"%BranchMiss."
"                Branches are accessed probably 100x more often than leaves.  I suspect <2% is a good number to aim for."
"%LeafMiss."
"                An indication of what proportion of the leaves are in the cache."
"%LeafFetch."
"                How often a leaf that wasn't in the cache had to be fetched from disk i.e. was not in the page cache."
""
"                For maximum THROUGHPUT it may be best to reduce %LeafMiss - since any leaf miss has a cpu overhead decompressing the leaves, and to a 1st order approximation"
"                disk read time should not affect throughput.  (The time for a leaf page-cache read is a good estimate of the cpu impact of reading from disk.)"
"                For minimum LATENCY you need to minimize %LeafMiss * (decompressTime + %LeafFetch * readTime).  Reducing the leaf cache size will increase leafMiss, but reduce leafFetch."
"                Because the pages are uncompressed in the page cache and compressed in the leaf cache the total number of reads will likely reduce."
"                If decompresstime << readTime, then it is more important to reduce %LeafMiss than %LeafFetch - which is particularly true of the new index format."
""
"avgTimeAgentProcess"
"                How long does it take to process a query on the agent?  Only accurate on 9.8.x and later."
""
"TimeLocalCpu    (TimeLocalExecute - TimeAgentWait - TimeSoapcall).  How much cpu time is spent on the server when processing the query?"
"RemoteCpuTime   (TimeAgentProcess - (TimeLeafRead - TimeBranchRead)).  How much cpu time is spent on the workers processing a query?"
""
"Load,Throughput"
"                The sum of these cpu times allows us to calculate an upper estimate of the cpu load and the maximum throughput (reported in the summary line).  If there is contention within the"
"                roxie process then the estimated cpu load will be higher than actual.  If so, it suggests some investigation of the roxie server is likely to improve throughput."
""
"MaxWorkerThreads"
"                Assuming the system is CPU bound, all index operations occur on the worker, and there is no thread contention (all bug assumptions), then given the stats for branch and leaf"
"                misses, what is the number of worker threads that would result in a cpu load that matches the number of cpus.  The actual limit can probably be slightly higher since there is"
"                variation in the number of requests which actually come from disk, but much higher is in danger of overcommitting the number of cpus.  Provides an initial ballpark figure."
"MaxTransactionsPerSecond, CpuLoad@10q/s"
"                For the specified number of cpus, what is the maximum number of transactions per second that could be supported.  What cpu load would you expect if queries were submitted at 10q/s."
"MaxServerThreads"
"                Taking into account the expected cpu load and time to process queries, how many server threads should be configured to support that throughput?  This is likely to be an under-estimate,"
"                but also an initial ball-park figure."
""
"From the final summary line:"
""
"Transactions    How many transactions in the sample, the elapsed time period, implied throughput and inverse time per transaction"
"ExpectedCpuLoad If there was no time lost to contention, what is the expected cpu load when running these queries?  If the actual load is much lower then that suggests there is opportunity"
"                for improving the roxie code to reduce contention and improve throughput/latency."
"iops            How many iops are required to support all the *actual* disk reads (not including those satisfied from page cache)"
"DiskThroughput  What rate does the disk need to support to transfer the data for all the queries?"
""
"Note: All times in ms unless explicitly stated"
'''
        if (args.commentary):
            print(commentary)

# Thoughts for future enhancements:
#
# Calculate average time for a leaf page-cache read and see how it affects the derived stats.
#
# Take into account NumAgentRequests and generate some derived stats.
#   avgQueue time - are the workers overloaded
#   TimeAgentWait v TimeAgentProcess?
#
# Add summary stats for the network traffic (needs SizeAgentRequests from 9.8.x)
#
# Calculate some "what if" statistics
# - If the decompression time was reduced by 50%
# - if the leaf cache hit 50% less, but reduced reads by 5% what is the effect?

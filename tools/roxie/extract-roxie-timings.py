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

def calculateDerivedStats(curRow):

    numBranchHits = float(curRow.get("NumNodeCacheHits", 0.0))
    numBranchAdds = float(curRow.get("NumNodeCacheAdds", 0.0))
    numBranchFetches = float(curRow.get("NumNodeDiskFetches", 0.0))
    timeBranchFetches = float(curRow.get("TimeNodeFetch", 0.0))
    if numBranchHits + numBranchAdds:
        curRow["%BranchMiss"] = 100*numBranchAdds/(numBranchAdds+numBranchHits)

    numLeafHits = float(curRow.get("NumLeafCacheHits", 0.0))
    numLeafAdds = float(curRow.get("NumLeafCacheAdds", 0.0))
    numLeafFetches = float(curRow.get("NumLeafDiskFetches", 0.0))
    timeLeafFetches = float(curRow.get("TimeLeafFetch", 0.0))
    if numLeafHits + numLeafAdds:
        curRow["%LeafMiss"] = 100*numLeafAdds/(numLeafAdds+numLeafHits)

    if numBranchAdds:
        curRow["%BranchFetch"] = 100*(numBranchFetches)/(numBranchAdds)

    if numLeafAdds:
        curRow["%LeafFetch"] = 100*(numLeafFetches)/(numLeafAdds)

    if numBranchFetches:
        curRow["AvgTimeBranchFetch"] = timeBranchFetches/(numBranchFetches)

    if numLeafFetches:
        curRow["AvgTimeLeafFetch"] = timeLeafFetches/(numLeafFetches)

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
    parser.add_argument("--nosummary", "-n", help="Avoid including a summary", action='store_true')
    parser.add_argument("--summaryonly", "-s", help="Only generate a summary", action='store_true')
    parser.add_argument("--ignorecase", "-i", help="Use case-insensitve query names", action='store_true')
    args = parser.parse_args()
    combineServices = args.all
    suppressDetails = args.summaryonly
    reportSummary = not args.nosummary or args.summaryonly
    ignoreQueryCase = args.ignorecase

    csv.field_size_limit(0x100000)
    with open(args.filename, encoding='latin1') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=' ')
        line_count = 0
        for row in csv_reader:
            numCols = len(row);
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
                if elapsedMatch:
                    curRow["elapsed"] =  int(elapsedMatch.group(1))

                #MORE: Unimplemented - allow timestamp filtering
                timestamp = ''
                for i in range(len(row)):
                    if yearMonthDayPattern.match(row[i]):
                        timestamp = row[i] + ' ' + row[i+1]
                        curRow["time"] = row[i+1]
                        break

                nesting = list()
                prefix = ''
                for cur in mapping:
                    if "=" in cur:
                        equals = cur.index('=')
                        name = prefix + cur[0:equals]
                        value = cur[equals+1:]
                        if value == '{':
                            nesting.append(prefix)
                            prefix += name + '.'
                        else:
                            allStats[name] = 1
                            castValue = -1

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
                    elif '}' == cur:
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

    # Create a string containing all the stats that were found in the file.
    headings =  'id'
    for statName in allStats:
        headings = headings + ',' + statName

    for service in allServices:
        allRows = allServices[service]

        # Calculate some derived statistics.
        for curRow in allRows:
            calculateDerivedStats(curRow)

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
            calculateDerivedStats(totalRow)

            # Average for all queries - should possibly also report average when stats are actually supplied
            numRows = len(allRows)
            avgRow = dict(_id_="avg", totalRow="avg")
            for statName in allStats:
                if statName in totalRow:
                    avgRow[statName] = float(totalRow[statName]) / numRows
            calculateDerivedStats(totalRow)

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

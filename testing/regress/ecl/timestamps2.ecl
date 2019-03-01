/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//nothor        - disable for the moment because of HPCC-21423
myWuid := WORKUNIT;

import lib_WorkunitServices.WorkunitServices;

ds := DATASET(10000, transform({ unsigned x => unsigned x2 }, self.x := COUNTER; SELF.x2 := COUNTER * COUNTER), DISTRIBUTED);

s := SORT(NOFOLD(ds), -x2);

d := DEDUP(s, x, LOCAL);

c := COUNT(NOFOLD(d));

action := OUTPUT(c - 10000);// == 0

countAll := COUNT(WorkunitServices.WorkunitStatistics(myWuid, true, ''));
countTimes := COUNT(WorkunitServices.WorkunitStatistics(myWuid, true, 'measure[ns]'));

numStartsA := SUM(WorkunitServices.WorkunitStatistics(myWuid, true, '')(name = 'NumStarts'), value);
numStartsB := SUM(WorkunitServices.WorkunitStatistics(myWuid, true, 'statistic[NumStarts]'), value);
numStopsA := SUM(WorkunitServices.WorkunitStatistics(myWuid, true, '')(name = 'NumStops'), value);
numStopsB := SUM(WorkunitServices.WorkunitStatistics(myWuid, true, 'statistic[NumStops]'), value);

sequential(
    action,
    nothor(
        ordered(
            output(countTimes < countAll);
            output(countTimes > 0);
            output(numStartsA > 0);
            output(numStartsA - numStartsB); // == 0 - since two ways of getting the same value
            output(numStopsA - numStopsB); // == 0 - since two ways of getting the same value
            output(numStartsA - numStopsB); // == 0; - if the graph completed normally
        )
    )
);

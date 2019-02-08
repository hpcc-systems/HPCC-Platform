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

//Disable comparison of the results for this test - results change too much - just ensure that it runs
//nokey
//nooutput

myWuid := WORKUNIT;

import lib_WorkunitServices.WorkunitServices;

ds := DATASET(10000, transform({ unsigned x => unsigned x2 }, self.x := COUNTER; SELF.x2 := COUNTER * COUNTER), DISTRIBUTED);

s := SORT(NOFOLD(ds), -x2);

d := DEDUP(s, x, LOCAL);

c := COUNT(NOFOLD(d));

action := OUTPUT(c - 10000);// == 0


stats := SEQUENTIAL(
output(WorkunitServices.WorkunitTimeStamps(myWuid)),
output(WorkunitServices.WorkunitTimings(myWuid)),
output(WorkunitServices.WorkunitStatistics(myWuid, false, '')),
output(WorkunitServices.WorkunitStatistics(myWuid, true, '')),
output(choosen(WorkunitServices.WorkunitStatistics(myWuid, true, ''), 2)),      // use choosen to partially read and ensure rows are cleared up
);

#if (__TARGET_PLATFORM__ = 'thorlcr')
   statsAction := NOTHOR(stats);
#else
   statsAction := stats;
#end

sequential(action, statsAction);

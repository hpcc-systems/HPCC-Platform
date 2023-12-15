/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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

//noroxie - seeks/wildseeks are ~ double in roxie - investigate
//nohthor - seeks/wildseeks are slightly different - investigate

//class=file
//class=index

#onwarning (4522, ignore); // ignore 'Implicit LIMIT(10000) added to keyed join'
#onwarning (4523, ignore); // ignore 'Neither LIMIT() nor CHOOSEN() supplied for index read on'
#onwarning (4515, ignore); // ignore 'keyed filter on wip follows unkeyed component wpos in the key'

import ^ as root;

// turn off node fetch timing, in order to get consistent results (inc. costs)
#option('nodeFetchThresholdNs', 0);

import common.JobStats;
 
kJStatKinds := DATASET([
   { 'NumIndexSeeks' }
  ,{ 'NumIndexScans' }
  ,{ 'NumIndexWildSeeks' }
], JobStats.jobStatKindRec);

kJFilter1 := 'scope[w2:graph2],nested[all],where[kind=159]';
kJFilter2 := 'scope[w2:graph3],nested[all],where[kind=159]';

SEQUENTIAL(
  COUNT(NOFOLD(JobStats.TestKJSimple()));
  COUNT(NOFOLD(JobStats.TestKJCQ()));
  OUTPUT(JobStats.getStats(WORKUNIT, kJFilter1, kJStatKinds), ALL);
  OUTPUT(JobStats.getStats(WORKUNIT, kJFilter2, kJStatKinds), ALL);
);

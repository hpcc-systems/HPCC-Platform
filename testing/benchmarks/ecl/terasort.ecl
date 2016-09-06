/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

// Perform global terasort

#option('THOR_ROWCRC', 0); // don't need individual row CRCs

#option('crcReadEnabled', false);
#option('crcWriteEnabled', false);
#option('timeActivities', false);
#option('optimizeLevel', 3);

rec := record
     string10  key;
     string10  seq;
     string80  fill;
       end;

in := DATASET('benchmark::terasort1',rec,FLAT);
OUTPUT(SORT(in,key,UNSTABLE),,'benchmark::terasort1out',overwrite);


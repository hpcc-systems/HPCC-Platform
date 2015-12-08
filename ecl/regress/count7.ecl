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

#option ('optimizeDiskSource', true);
#option ('countFile', false);
#option ('targetClusterType', 'roxie');

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable1 := dataset('x1',namesRecord,FLAT);
output(count(namesTable1) <= 5);
namesTable2 := dataset('x2',namesRecord,FLAT);
output(count(namesTable2) < 5);
namesTable3 := dataset('x3',namesRecord,FLAT);
output(count(namesTable3) > 5);
namesTable4 := dataset('x4',namesRecord,FLAT);
output(count(namesTable4) >= 5);
namesTable5 := dataset('x5',namesRecord,FLAT);
output(count(namesTable5) = 5);
namesTable6 := dataset('x6',namesRecord,FLAT);
output(count(namesTable6) != 5);
namesTable7 := dataset('x7',namesRecord,FLAT);
output(count(namesTable7, HINT(goQuitefast(true))) != 5);
output(count(namesTable7, HINT(goReallyReallyFast(true))) > 0);

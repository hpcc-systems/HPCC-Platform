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

#option ('targetClusterType', 'roxie');

namesRecord :=
            RECORD
string20        a;
string10        b;
integer2        c;
integer2        d;
            END;

namesTable := dataset('x', namesRecord, thor);

s1 := sort(namesTable, a, b, c);
g1 := group(s1, a, d);
s2 := sort(g1, c, d);
g2 := group(s2);
// sort order of g2 should be a, <unknown>
s3 := sort(g2, a, c, d);        // invalid to be optimized away
output(s3);

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

#option ('targetClusterType', 'hthor');

idRecord := { unsigned id; };

inRecord := RECORD
unsigned        box;
dataset(idRecord) ids;
            END;

inTable := dataset([
        {1, [{1},{2},{3}]},
        {2, [{1},{3},{4}]},
        {2, [{2},{3},{4}]}
        ], inRecord);

outRecord := RECORD
dataset(idRecord) joined;
string cnt;
unsigned i;
             END;

outRecord t1(outRecord prev, inRecord next) := TRANSFORM
    isFirst := NOT EXISTS(prev.joined);
    SELF.joined := next.ids;
    SELF.cnt := (string)((unsigned)prev.cnt + IF(isFIRST, 1, 2) + COUNT(prev.joined) + MAX(prev.joined, id));
    SELF.i := COUNT(prev.joined);
END;

output(AGGREGATE(inTable, outRecord, t1(RIGHT, LEFT)));

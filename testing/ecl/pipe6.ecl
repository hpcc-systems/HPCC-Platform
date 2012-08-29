/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

import lib_stringlib;
import std.str;

outRec := { STRING lout; };

d := dataset([{ 'Hello there'}, {'what a nice day'}, {'1234'}], { string line}) : stored('nofold');

p1 := PIPE(d(line!='p1'), 'sort', outRec, csv, output(xml), repeat);
p2 := PIPE(d(line!='p2'), 'sort', outRec, csv, output(xml), repeat, group);

output(p1, { count(group)} );
output(p2, { count(group)} );

outRec concat(outRec l, outRec r) := TRANSFORM
    SELF.lout := r.lout + l.lout;
END;

output(AGGREGATE(p2, outRec, concat(LEFT, RIGHT)));

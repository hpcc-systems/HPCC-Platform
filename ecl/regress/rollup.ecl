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


namesRecord :=
            RECORD
integer2        age := 25;
string20        surname;
string10        forename;
            END;

namesTable := dataset('x',namesRecord,FLAT);

namesRecord t(namesRecord l) := TRANSFORM SELF:= l; END;

groupedNamesTable := group(namesTable, forename);
o1 := rollup(groupedNamesTable, (string10)age,t(LEFT));
o2 := rollup(group(o1), (integer4)surname, t(LEFT));
o3 := rollup(o2, LEFT.age-RIGHT.age>8, t(LEFT), LOCAL);

output(o3,,'out.d00');

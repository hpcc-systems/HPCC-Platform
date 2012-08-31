/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    This program is free software: you can redistribute it and/or modify
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

import macro7b;

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

macro7b.z(namesTable, xsum);

output(xsum);


xsum2 := @FUNCTION
    z := namesTable(age != 10);
    return z + z;
END + namesTable;
output(xsum2);


xsum4 := @FUNCTION
    return table(group(namesTable, age), {count(gr)});
END;

output(xsum4);


xsum3 := macro7b.z2(namesTable);

output(xsum3);


split := macro7b.ageSplit(namesTable);
output(split.young + split.old);

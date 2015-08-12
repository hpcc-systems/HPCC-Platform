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
integer2        f1;
integer2        f2;
integer2        f3;
integer2        f4;
integer2        f5;
integer2        f6;
            END;

namesTable := dataset('x',namesRecord,FLAT);

deduped := dedup(namesTable, ((string)f1)+((string)f2)+((string)f3)+((string)f4)+((string)f5)+((string)f6));

output(deduped,,'out.d00');

groupedNames := group(namesTable, ((string)f1)+((string)f2)+((string)f3)+((string)f4)+((string)f5)+((string)f6));
output(table(groupednames,{count(group)}),,'out.d01');

sortedNames := sort(namesTable, ((string)f1)+((string)f2)+((string)f3)+((string)f4)+((string)f5)+((string)f6));
output(sortedNames,,'out.d02');

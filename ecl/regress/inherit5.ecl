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

//BUG: 13645 Check maxlength is inherited correctly
//It should really extend it if child only contains fixed length fields, but that isn't implemented yet.

namesRecord :=
            RECORD,maxlength(12345)
string          surname;
string          forename;
            END;

personRecord :=
            RECORD(namesRecord)
integer2        age := 25;
            END;

personRecordEx :=
            RECORD(personRecord), locale('Somewhere')
unsigned8       filepos{virtual(fileposition)};
            END;

personRecord2 :=
            RECORD(namesRecord),maxlength(1000)
integer2        age := 25;
            END;

output(dataset('x',personRecord,FLAT));
output(dataset('x1',personRecordEx,FLAT));
output(dataset('x',personRecord2,FLAT));

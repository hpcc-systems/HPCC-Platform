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

TestRecord :=   RECORD
string20            surname;
unsigned1           flags;
                    IFBLOCK(SELF.flags & 1 <> 0)
string20                forename;
                    END;
                    IFBLOCK(SELF.flags & 2 <> 0)
integer4                age := 0;
string8                 dob := '00001900';
                    END;
                END;


TestData := DATASET('if.d00',testRecord,FLAT);


OUTPUT(CHOOSEN(TestData,100),{surname,forename,age,dob},'out.d00');


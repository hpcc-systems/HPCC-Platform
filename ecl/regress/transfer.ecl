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

#option ('foldAssign', false);
#option ('globalFold', false);
testRecord := RECORD
string10            forename;
integer4            surname;
string2             strsalary;
string2             nl;
                END;

testDataset := DATASET([{'a','a','a','a'}], testRecord);

newDataset := table(testDataset, {
    transfer('gavin',string4),
    (string3)transfer('A',unsigned1),
    transfer(99%256,string1)});

/*
newDataset := table(testDataset, {
    transfer('01',integer2),
    transfer(forename,integer4),
    transfer(forename[1..4],integer4),
    transfer(surname,string4),
    transfer(surname,integer2),
    transfer('gavin',string4)});
*/

output(newDataset,,'out.d00');

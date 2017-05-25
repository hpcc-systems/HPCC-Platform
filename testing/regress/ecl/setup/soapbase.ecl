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

//nohthor
//nothor
//publish

string10 unkname := 'Unknown' : stored ('UnkName');

childRecord := record
unsigned            id;
    end;

ServiceOutRecord :=
    RECORD
        unsigned1 r;
        data pic;
        string15 name;
        dataset(childRecord) ids{maxcount(5)};
    END;


IF (unkname='FAIL', FAIL(1,'You asked me to fail'),
  output(dataset([
            {1, x'0102', 'RICHARD', [{1}]},
            {2, x'0102030405', 'LORRAINE', [{8},{7},{6}]},
            { 3, x'5432', unkname, []}
            ], ServiceOutRecord)));

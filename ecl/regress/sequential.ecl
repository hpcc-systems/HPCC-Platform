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

personRecord := RECORD
unsigned4 personid;
string1 sex;
string33 forename;
string33 surname;
string4 yob;
unsigned4 salary;
string20 security;
string1 ownsRentsBoard;
unsigned2 numPublic;
unsigned1 numNotes;
unsigned2 numTrade;
unsigned4 person_sumtradeid;
unsigned4 public_c;
unsigned4 trade_c;
    END;
personDataset := DATASET('person',personRecord,FLAT);


o1 := output(personDataset,,'out1.d00');
o2 := output(personDataset,,'out2.d00');
o3 := output(personDataset,,'out3.d00');
o4 := output(personDataset,,'out4.d00');

o1;
o2;
sequential(o1, o2);

sequential(parallel(o2,o3),parallel(o1,o4));




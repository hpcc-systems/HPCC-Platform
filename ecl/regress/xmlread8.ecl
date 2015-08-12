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

nameRecord :=
                RECORD
string              idx{xpath('@index')};
string              name{xpath('')};
                END;

instrumentRecord := RECORD,maxLength(10000)
udecimal10              id{xpath('@id')};
integer                 book{xpath('BOOK')};
dataset(nameRecord)     names{xpath('NAME')};
set of string           legals{xpath('LEGAL')};
dataset(nameRecord)     names2;
set of string           legals2;
dataset(nameRecord)     names3{xpath('NAMES/NAME')};
set of string           legals3{xpath('LEGALS/LEGAL')};
set of string           legals4{xpath('LEGALS/LEGAL[@type=\'legal\']/@xyz')};
set of string           legals5{xpath('/LEGAL/@xyz')};
                END;


test := dataset('~file::127.0.0.1::temp::20040621_vclerk.xml', instrumentRecord, XML('/VolusiaClerk/OfficialRecords/Instrument'));
output(choosen(test,100),,'out2.d00',XML,overwrite);

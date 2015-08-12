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

phoneRecord :=
            RECORD
string5         areaCode{xpath('@areaCode')};
string12        number{xpath('@number')};
            END;

nullPhones := dataset('ph', phoneRecord, thor);

contactrecord :=
            RECORD
phoneRecord     phone;
boolean         hasemail;
                ifblock(self.hasemail)
string              email;
                end;
            END;

personRecord :=
            RECORD
string20        surname;
string20        forename;
set of string   middle{xpath('MiddleName/Instance')} := [];
//dataset(phoneRecord) phones{xpath('/Phone')} := dataset([{ '', ''}], phoneRecord)(false);
dataset(phoneRecord) phones{xpath('/Phone')} := _EMPTY_(phoneRecord); //nullPhones;
            END;

namesTable := dataset([
        {'Hawthorn','Gavin'},
        {'Hawthorn','Abigail'}
        ], personRecord);

personRecord t(personrecord l) := transform
        SELF.middle := IF(l.forename = 'Gavin', ['Charles'], ['Anabelle','Spandex']);
        SELF := l;
        END;

namesTable2 := project(namesTable, t(left));
output(namesTable2,,'people.xml',overwrite,xml);

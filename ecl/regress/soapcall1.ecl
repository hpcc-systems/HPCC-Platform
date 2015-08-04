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
string5         areaCode;
string12        number;
            END;

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
            END;

namesTable := dataset([
        {'Hawthorn','Gavin',['Charles']},
        {'Hawthorn','Abigail',['Anabelle','Spandex']},
        {'Hawthorn','',[]},
        {'Smith','John',ALL}
        ], personRecord);

output(namesTable,,'people.xml',overwrite,xml(trim,opt));

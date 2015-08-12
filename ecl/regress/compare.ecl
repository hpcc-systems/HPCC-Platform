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
namesRecord :=
            RECORD
string20        surname;
string10        forename;
varstring20     vsurname;
integer2        age := 25;
            END;

names := dataset([{'Hawthorn','Gavin','Hawthorn',10}],namesRecord);

output(nofold(names),{
        'ab'='ab',
        'ab' = 'ab ',
        'ab' = 'ab '[1..3+0],
        'ab'[1..2+0] = 'ab ',
        'ab'[1..2+0] = 'ab '[1..3+0],
        trim(surname)=vsurname,
        true
        },'out.d00');


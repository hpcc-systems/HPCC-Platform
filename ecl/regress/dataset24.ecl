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
string10        surname;
string10        forename;
integer2        age := 25;
            END;

unsigned zero := 0 : stored('zero');
namesTable2 := dataset([
        {'Hawthorn','Gavin',31+zero},
        {'Hawthorn','Peter',30+zero},
        {'Smithe','Simon',10+zero},
        {'X','Z',zero}], namesRecord, HINT(ThisIsAHint(true)));

output(namesTable2,,'out.d00',overwrite);

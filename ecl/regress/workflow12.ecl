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
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable1 := dataset([
        {'Hawthorn','Gavin',31},
        {'X','Z'}], namesRecord);

o1 := output(namesTable1,,'~names',overwrite);


namesTable2 := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',31},
        {'Hawthorn','Robert',0},
        {'X','Z'}], namesRecord);

o2 := output(namesTable2,,'~names',overwrite);



namesTable := dataset('~names',namesRecord,FLAT);

p1 := namesTable(age <> 0) : success(o1);
p2 := p1(age <> 10) : success(o1, LABEL('When things go right'));

output(p2);

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
        {'Smith','Gavin',31},
        {'Smythe','Mia',30},
        {'Smiph','Pru',10},
        {'Y','Z'}], namesRecord);

namesTable2 := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

boolean bool1 := true : stored('bool1');


ds3 := DATASET([1,2,3,4,10,99],{integer searchAge});

r := { integer age; string surname; };

r t(ds3 l) := TRANSFORM
    ds := IF(l.searchAge % 2 = 1, namesTable1, namesTable2);
    SELF.age := l.searchAge;
    SELF.surname := ds(age = l.searchAge)[1].surname;
END;

p := PROJECT(ds3, t(LEFT));

output(p);

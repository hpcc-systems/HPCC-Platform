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

zpr :=
            RECORD
string20        forename;
string20        surname;
unsigned4       age;
            END;

zperson := dataset([
        {'Gavin','Hawthorn',32},
        {'Jason','Hawthorn',28},
        {'Mia','Malloy',28},
        {'Mia','Stevenson',32},
        {'James','Mildew',46},
        {'Arther','Dent',60},
        {'Mia','Hawthorn',31},
        {'Ronald','Regan',84},
        {'Mia','Zappa',12}
        ], zpr);


x := table(zperson, {count(group),min(group,age),trim(surname)}, trim(surname), few);
output(x);

y := table(zperson, {count(group),age}, age, few);
output(y);

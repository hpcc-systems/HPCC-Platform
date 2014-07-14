/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
        {'Gavin','Halliday',32},
        {'Jason','Halliday',28},
        {'Liz','Malloy',28},
        {'Liz','Stevenson',32},
        {'James','Mildew',46},
        {'Arther','Dent',60},
        {'Liz','Halliday',31},
        {'Ronald','Regan',84},
        {'Liz','Zappa',12}
        ], zpr);

sortedzperson := sort(zperson, surname, age);

x := choosesets(sortedzperson, surname='Halliday'=>2,forename='Liz'=>3,1,EXCLUSIVE);
output(x);

y := choosesets(sortedzperson, surname='Halliday'=>2,forename='Liz'=>3,1);
output(y);

x1 := choosesets(sortedzperson, surname='Halliday'=>2,forename='Liz'=>3,1,ENTH);
output(x1);

y1 := choosesets(sortedzperson, surname='Halliday'=>2,forename='Liz'=>3,LAST);
output(y1);


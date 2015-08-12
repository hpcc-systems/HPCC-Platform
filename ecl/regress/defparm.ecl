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

#option ('globalFold', false);
import jfgao;

export myMacro(t1 = '1', t2 = '2 + 3', t3 = '\'a\\\'c\' + \'34\'') :=
  macro
    t1+t2+t3
  endmacro;

mymacro();
mymacro(,'4');
myMacro(,,200);
myMacro(1);
mymacro(1,2);
mymacro(1,,2);
mymacro(,1,2);

person := dataset('person', { unsigned8 person_id, string1 per_sex, string10 per_ssn }, thor);
shared tt := person;
export myMacrox(inputTable = 'tt') :=
  macro
    count(inputTable)
  endmacro;

export test_myMacrox := myMacrox();

test_myMacrox;

jfgao.test_myMacro();

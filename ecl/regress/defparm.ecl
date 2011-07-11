/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

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



integer8 x1(string5 a) := case(a,
            'one  '=>1,
            'two  '=>2,
            'three'=>3,
            'four '=>4,
            'five '=>5,
            'six  '=>6,
            'seven'=>7,
            'eight'=>8,
            'nine '=>9,
            'ten  '=>10,
            0);

five := 'five' : stored('five');

output(x1(five));

integer8 x2(string5 a) := case(a,
            'eight'=>1,
            'five '=>2,
            'four '=>3,
            'nine '=>4,
            'one  '=>5,
            'seven'=>6,
            'six  '=>7,
            'ten  '=>8,
            'three'=>9,
            'two  '=>10,
            0);


output(x2(five));

integer8 x3(string5 a) := case(a,
            'eight'=>10000000000000,
            'five '=>20000000000000,
            'four '=>30000000000000,
            'nine '=>40000000000000,
            'one  '=>50000000000000,
            'seven'=>60000000000000,
            'six  '=>70000000000000,
            'ten  '=>80000000000000,
            'three'=>90000000000000,
            'two  '=>100000000000000,
            0);


output(x3(five));

integer2 x4(string5 a) := case(a,
            'eight'=>1,
            'five '=>2,
            'four '=>3,
            'nine '=>4,
            'one  '=>5,
            'seven'=>6,
            'six  '=>7,
            'ten  '=>8,
            'three'=>9,
            'two  '=>10,
            0);


output(x4(five));

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

inRecord :=
            RECORD
unsigned        box;
string          text{maxlength(10)};
            END;

inTable := dataset([
        {1, 'Shoe'},
        {1, 'Feather'},
        {1, 'Cymbal'},
        {1, 'Train'},
        {2, 'Envelope'},
        {2, 'Pen'},
        {2, 'Jumper'},
        {3, 'Dinosoaur'},
        {3, 'Dish'}
        ], inRecord);

outRecord1 := RECORD
                STRING          contents{maxlength(200)};
             END;

outRecord2 := RECORD(outRecord1)
    unsigned        id;
END;

//MORE: This could be optimized to just extend the field in place
transform(outRecord1) t1(inRecord l, outRecord1 r) := BEGINC++
    return 0;   //t1()
ENDC++;

transform(outRecord1) merge1(outRecord1 r1, outRecord1 r2) := BEGINC++
    return 0;   //merge...()
ENDC++;

transform(outRecord2) t2(inRecord l, outRecord2 r) := BEGINC++
    return 0;   //t2()
ENDC++;


o1 := output(AGGREGATE(inTable, outRecord1, t1(LEFT, RIGHT), LOCAL));
o2 := output(AGGREGATE(inTable, outRecord2, t2(LEFT, RIGHT), LEFT.box, LOCAL));
o3 := output(AGGREGATE(inTable, outRecord2, t2(LEFT, RIGHT), LEFT.box, FEW, LOCAL));
o4 := output(AGGREGATE(inTable, outRecord1, t1(LEFT, RIGHT), merge1(RIGHT1, RIGHT2)));

SEQUENTIAL(o1, o2, o3, o4);

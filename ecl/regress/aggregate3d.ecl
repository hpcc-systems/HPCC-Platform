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
                STRING          contents2{maxlength(200)};
             END;

outRecord2 := RECORD(outRecord1)
    unsigned        id;
    '\n';
END;

//MORE: This could be optimized to just extend the field in place
outRecord1 t1(inRecord l, outRecord1 r) := TRANSFORM
    SELF.contents := r.contents + IF(r.contents <> '', ',', '') + l.text;
    SELF.contents2 := r.contents2 + ',' + l.text;
END;

output(AGGREGATE(inTable, outRecord1, t1(LEFT, RIGHT)));

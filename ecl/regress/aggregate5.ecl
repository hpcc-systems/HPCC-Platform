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

inTable1 := dataset([
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

output(inTable1,,'~testing::boxData');
inTable := dataset('~testing::boxData', inRecord, thor);

outRecord1 := RECORD
                STRING          contents{maxlength(200)};
                STRING          contents2{maxlength(200)};
             END;

outRecord2 := RECORD(outRecord1)
    unsigned        id;
    '\n';
END;

outRecord3 := RECORD
    unsigned        id;
    outRecord1;
    '\n';
END;

//MORE: This could be optimized to just extend the field in place
outRecord1 t1(inRecord l, outRecord1 r) := TRANSFORM
    SELF.contents := r.contents + (l.text + ' ');
    SELF.contents2 := r.contents2 + (l.text[1..2] + ' ');
END;

outRecord2 t2(inRecord l, outRecord2 r) := TRANSFORM
    SELF.contents := r.contents + (l.text + ' ');
    SELF.contents2 := r.contents2 + (l.text[1..2] + ' ');
    SELF.id := l.box;
END;

outRecord3 t3(inRecord l, outRecord3 r) := TRANSFORM
    SELF.contents := r.contents + (l.text + ' ');
    SELF.contents2 := (l.text[1..2] + ' ') + r.contents2;
    SELF.id := l.box;
END;

o1 := output(AGGREGATE(inTable, outRecord1, t1(LEFT, RIGHT)));
o2 := output(AGGREGATE(inTable, outRecord2, t2(LEFT, RIGHT), LEFT.box));
o3 := output(sort(nofold(AGGREGATE(inTable, outRecord2, t2(LEFT, RIGHT), LEFT.box, FEW)),id));
o4 := output(sort(nofold(AGGREGATE(inTable, outRecord3, t3(LEFT, RIGHT), LEFT.box, FEW)),id));

SEQUENTIAL(output('o1\n'),o1,output('\no2\n'),o2,output('\no3\n'),o3,output('\no4\n'),o4);

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

outRecord4 := RECORD
    unsigned        id;
    '\n';
    unsigned4       magiclen;
    '\n';
    unsigned        maxlen;
    '\n';
END;


outRecord4 t4(inRecord l, outRecord4 r) := TRANSFORM
    SELF.magiclen := 10 * r.magiclen * LENGTH(TRIM(l.text));
    SELF.maxlen := SUM(8, r.maxlen, LENGTH(TRIM(l.text)));
    SELF.id := l.box;
END;

o1 := output(sort(nofold(AGGREGATE(inTable, outRecord4, t4(LEFT, RIGHT), LEFT.box, FEW)),magiclen));

SEQUENTIAL(output('o1\n'),o1);

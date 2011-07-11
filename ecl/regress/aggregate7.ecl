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

itemRecord := RECORD
string          text{maxlength(10)};
            END;

outRecord := RECORD
unsigned        box;
dataset(itemRecord) items;
             END;

//Need to make sure items isn't released since not assigned in the record.
outRecord t1(inRecord l, outRecord r) := TRANSFORM
    SELF.box := l.box;
    SELF.items := row(TRANSFORM(itemRecord, SELF.text := l.text));
END;

groupedByBox := group(inTable, box);
output(AGGREGATE(groupedByBox, outRecord, t1(LEFT, RIGHT)));

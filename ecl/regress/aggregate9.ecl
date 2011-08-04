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
unsigned        mask;
            END;

inTable := dataset([
        {1, 0x11},
        {1, 0x33},
        {1, 0xDD},
        {1, 0x11},
        {2, 0x0F},
        {2, 0xF0},
        {2, 0x137F},
        {3, 0xf731},
        {3, 0x11FF}
        ], inRecord);

outRecord1 := RECORD
    unsigned        box;
    unsigned        maskor;
    unsigned        maskand := 0xffffffffff;
             END;

outRecord1 t1(inRecord l, outRecord1 r) := TRANSFORM
    SELF.box := l.box;
    SELF.maskor := r.maskor | l.mask;
    SELF.maskand := IF(r.box <> 0, r.maskand, 0xffffffff) & l.mask;
END;

output(AGGREGATE(inTable, outRecord1, t1(LEFT, RIGHT), LEFT.box));

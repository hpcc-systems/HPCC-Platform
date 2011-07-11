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

aRecord := RECORD
unsigned8 __filepos{VIRTUAL(FILEPOSITION)};
data9 per_cid;
    END;

aDataset := DATASET('a',aRecord,FLAT);
a1 := aDataset(per_cid<>x'012345678901234567');
//output(a1,, 'outa.d00');

aRecord trans(aRecord l) := TRANSFORM
   SELF.per_cid := l.per_cid[1..3]+x'000000'+l.per_cid[7..9];
   SELF := l;
    END;

aRecord trans2(aRecord l) := TRANSFORM
   SELF.__filepos := l.__filepos + 100;
   SELF := l;
    END;

a2 := project(a1, trans(LEFT));
//output(a2,, 'outa.d00');

a3 := project(a2, trans2(LEFT));
output(a3,, 'outa.d00');

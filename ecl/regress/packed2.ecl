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

personRecord := RECORD
packed unsigned2 l2;
packed unsigned1 l1;
packed    unsigned2 b2;
packed    unsigned1 b1;
    END;

personDataset := DATASET('person',personRecord,FLAT);

personRecord t1(personRecord l) := TRANSFORM
        SELF.l2 := l.b1;
        SELF.l1 := l.b2;
        SELF.b2 := l.l1;
        SELF.b1 := l.l2;
        SELF := l;
    END;

o1 := project(personDataset, t1(LEFT));
output(o1,,'out1.d00');

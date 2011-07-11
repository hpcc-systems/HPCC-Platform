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


thin :=     RECORD
unsigned4       f1;
string20        f2;
                ifblock(self.f1 > 10)
qstring10           f3;
                end;
            END;

fat :=      RECORD
thin            g1;
thin            g2;
thin            g3;
            END;


inThin := dataset('thin', thin, thor);
inFat := dataset('fat', fat, thor);

fat fillit(fat l, thin r, unsigned c) := transform
        self.g1 := if(c = 1, r, l.g1);
        self.g2 := if(c = 2, r, l.g2);
        self.g3 := if(c = 3, r, l.g3);
    end;

o := denormalize(inFat, inThin, left.g1.f1 = right.f1, fillit(left, right, counter));

output(o);

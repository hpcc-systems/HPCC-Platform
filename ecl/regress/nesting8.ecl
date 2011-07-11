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

import dt;

thin :=     RECORD
unsigned4       f1;
string20        f2;
            END;

inThin := dataset('thin', thin, thor);

fat1 :=     RECORD
thin            g1;
thin            g2;
unsigned        g3 := inThin.f1;
                ifblock(self.g3 > 100)
unsigned            g4;
                end;
            END;


fat2 :=     RECORD
thin            g1;
thin            g2;
unsigned        g3 := inThin.f1;
            END;

inFat1 := dataset('fat1', fat1, thor);

x := sort(inFat1, g3*2, g4);
output(x);

inFat2 := dataset('fat2', fat2, thor);
y := sort(inFat2, g1.f1, g2.f2);
output(y);

z := sort(inFat2, g3);
output(z);

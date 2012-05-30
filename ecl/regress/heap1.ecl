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

import std.System.Debug;

startTime := Debug.msTick() : independent;

namesRecord :=
            RECORD
string         x;
            END;

namesRecord t(unsigned c) := TRANSFORM
    len := [4,16,32,64][(c % 4) + 1];
    SELF.x := 'x'[1..len];
END;

numIter := 1000000;
ds := NOFOLD(DATASET(numIter, t(COUNTER)));

output(COUNT(ds));

output('Time taken = ' + (string)(Debug.msTick()-startTime));

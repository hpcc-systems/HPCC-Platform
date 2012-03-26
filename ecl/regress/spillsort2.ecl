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


inputRecord := { unsigned4 id1, unsigned4 id2 };

inputRecord createRecord(unsigned c) := TRANSFORM
    SELF.id1 := (c-1) DIV 10000;
    SELF.id2 := HASH64(c);
END;

unsigned numRows := 20000000;

ds := dataset(numRows, createRecord(COUNTER));

sortedDs := SORT(NOFOLD(ds), id1, id2, UNSTABLE('spillingquicksort'));
counted1 := COUNT(NOFOLD(sortedDs));
output(counted1);

shuffledDs := UNGROUP(SORT(GROUP(NOFOLD(ds), id1),id2, UNSTABLE('spillingquicksort')));
counted2 := COUNT(NOFOLD(shuffledDs));
output(counted2);

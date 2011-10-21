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

#option ('targetClusterType', 'hthor');

idRecord := { unsigned id; };

inRecord := RECORD
unsigned        box;
dataset(idRecord) ids;
            END;

inTable := dataset([
        {1, [{1},{2},{3}]},
        {2, [{1},{3},{4}]},
        {2, [{2},{3},{4}]}
        ], inRecord);

outRecord := RECORD
dataset(idRecord) joined;
string cnt;
unsigned i;
             END;

outRecord t1(outRecord prev, inRecord next) := TRANSFORM
    isFirst := NOT EXISTS(prev.joined);
    SELF.joined := IF(isFirst, next.ids, JOIN(next.ids, prev.joined, LEFT.id = RIGHT.id));
    SELF.cnt := (string)((unsigned)prev.cnt + IF(isFIRST, 1, 2) + COUNT(prev.joined) + MAX(prev.joined, id));
    SELF.i := COUNT(prev.joined);
END;

output(AGGREGATE(inTable, outRecord, t1(RIGHT, LEFT)));

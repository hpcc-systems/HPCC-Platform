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

#option ('targetClusterType', 'roxie');

namesRecord :=
            RECORD
string20        a;
string10        b;
integer2        c;
integer2        d;
            END;

namesTable := dataset('x', namesRecord, thor);

s1 := sort(namesTable, a, b, c);
g1 := group(s1, a, d);
s2 := sort(g1, c, d);
g2 := group(s2);
// sort order of g2 should be a, <unknown>
s3 := sort(g2, a, c, d);        // invalid to be optimized away
output(s3);

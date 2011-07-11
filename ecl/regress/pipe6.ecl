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

import lib_stringlib;
import std.str;

outRec := { STRING lout; };

d := dataset([{ 'Hello there'}, {'what a nice day'}, {'1234'}], { string line}) : stored('nofold');

p1 := PIPE(d(line!='p1'), 'sort', outRec, csv, output(xml), repeat);
p2 := PIPE(d(line!='p2'), 'sort', outRec, csv, output(xml), repeat, group);

output(p1, { count(group)} );
output(p2, { count(group)} );

outRec concat(outRec l, outRec r) := TRANSFORM
    SELF.lout := r.lout + l.lout;
END;

output(AGGREGATE(p2, outRec, concat(LEFT, RIGHT)), { lout, '!\n' });


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

d := dataset([{ 'Hello there'}, {'what a nice day'}, {'1234'}], { string line}) : stored('nofold');

p1 := PIPE(d(line!='p1'), 'sort', { string lout }, csv, output(csv));
p2 := PIPE(d(line!='p2'), 'sort', { string lout }, csv, output(xml));
p3 := PIPE(d(line!='p2'), 'sort', { string lout }, csv, output(xml), repeat);

output(p1, { string l := Str.FindReplace(lout, '\r', ' '), '\n' } );
output(p2, { string l := Str.FindReplace(lout, '\r', ' '), '\n' } );
output(p3, { string l := Str.FindReplace(lout, '\r', ' '), '\n' } );

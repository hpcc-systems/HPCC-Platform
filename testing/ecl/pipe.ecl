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

export mdstring(string del) := TYPE  
  export integer physicallength(string s) := lib_stringlib.StringLib.StringUnboundedUnsafeFind(s,del)+length(del)-1;
  export string load(string s) := s[1..lib_stringlib.StringLib.StringUnboundedUnsafeFind(s,del)-1];
  export string store(string s) := s + del;
END;

d := dataset([{ 'Hello there'}, {'what a nice day'}, {'1234'}], { varstring20 line}) : stored('nofold');

dt := TABLE(d, { mdstring('\n') l := line });

p1 := PIPE(dt(l!='p1'), 'sort', { mdstring('\n') lout });
p2 := PIPE(dt(l!='p2'), 'sort', { mdstring('\n') lout }, REPEAT);
p3 := CHOOSEN(PIPE(dt(l!='p3'), 'sort', { mdstring('\n') lout }), 1);
p4 := CHOOSEN(PIPE(dt(l!='p4'), 'sort', { mdstring('\n') lout }, REPEAT), 1);

output(p1, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
output(p2, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
output(p3, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
output(p4, { string20 l := Str.FindReplace(lout, '\r', ' ') } );



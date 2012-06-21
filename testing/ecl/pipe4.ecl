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

//Number of leading spaces will force the lines to come out in the correct order
d := dataset(['</Zingo>', ' <Zango>Line3</Zango>', '  <Zango>Middle</Zango>', '   <Zango>Line1</Zango>', '    <Zingo>' ], { string line}) : stored('nofold');

#IF (__OS__ = 'windows')
pipeCmd := 'sort';
#ELSE
pipeCmd := 'sh -c \'export LC_ALL=C; sort\'';
#END
p1 := PIPE(d(line!='p1'), pipeCmd, { string lout{XPATH('')} }, xml('Zingo/Zango'), output(csv));

output(p1, { string l := Str.FindReplace(lout, '\r', ' ') } );



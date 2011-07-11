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

r := record
   dt.ebcdic_pstring  a;
   end;

//d := dataset([{x'05'},{x'e6'},{x'C9'},{x'D5'},{x'C7'},{x'E2'}],r);

//output(d,,'temp.dab');

string load(ebcdic string x) := x[2..transfer(x[1], unsigned1)+1];

d := dataset('temp.dab',r,flat);

r1 := record
  string6 a;
  end;

r1 trans(r l) := transform
  self.a := l.a;
  end;

p := project(d,trans(left));

output(p)

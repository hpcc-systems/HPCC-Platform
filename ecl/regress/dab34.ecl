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


pstring := TYPE
    export integer physicallength(string s) := transfer(s[1],unsigned
integer1);
    export string load(string s) := s[2..transfer(s[1],unsigned
integer1)];
    export string store(string s) := s;
    END;

r := record
  pstring p;
  end;

r1 := record
  unsigned integer1 u;
  end;
/*d := dataset([{3},{60},{61},{62}],r1);

output(d,r1,'temp.dab'); */
d := dataset('temp.dab',r,flat);

r2 := record
  string20 p;
  end;

r2 trans(r le) := transform
  self := le;
  end;

pr := project(d,trans(left));

output(pr);

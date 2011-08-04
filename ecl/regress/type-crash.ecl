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

pstring := type
   export integer physicallength(string x) transfer(x[1],unsigned1)+1;
   export string load(string x) := x[2..transfer(x[1],unsigned1)+1];
   export string store(string x) :=
end;

epstring := type
   export integer physicallength(ebcdic string x) := transfer(x[1],unsigned1)+1;    export string load(ebcdic string x) := x[2..transfer(x[1],unsigned1)+1];    export ebcdic string store(string := transfer(length(x),string1)+x;
end;

r := record
   unsigned1 flags;    ifblock(self.flags & 1 != 0)
      pstring a;
   end;
   ifblock(self.flags & != 0)
     epstring b;
   end;
   ifblock(self.flags & 2 != 0)
      ebcdic string1 c;
   end;
end;

d := dataset('in.d00', r,FLAT);

r1 := record
   string100 a := d.a;
   string100 b := d.b;    string1 c := d.c;
end;

output(d,r1);


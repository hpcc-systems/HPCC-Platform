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
  export integer physicallength(string x) := transfer(x[1],unsigned1)+1;
  export string load(string x) := x[2..transfer(x[1],unsigned1)+1];
  export string store(string x) := transfer(length(x),string1)+x;
  end;

r := record
     pstring a;
     end;

//d := dataset('in.d00', r,FLAT);
d := dataset([{7},{83},{85},{67},{67},{69},{83},{83}],r);
//d := dataset([{7}],r);

r1 := record
  string100 a1 := d.a;
      end;

output(nofold(d),r1);



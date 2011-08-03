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


r := record
  string line;
  end;

ds := dataset([
{'abcabcabcabcabcabcabcabcabcabcabcabcabcabcabc'},
{'abcABCabcABCabcabcABCabcABCabcabcABCabcABCabc'},
{'AabBcCdDeEfF'},
{''}],r);

pattern a := 'abc';

pattern b := a nocase(a) a;

pattern S := b;

results :=
    record
        string x := MATCHTEXT;
        string y := MATCHTEXT(a);
    end;

output(PARSE(ds,line,S,results,scan all));


pattern b2 := validate(a, matchtext != '');

pattern c2 := b2 nocase(b2) b2;

pattern S2 := c2;

output(PARSE(ds,line,S2,results,scan all));

pattern z := ['a','b','c'] or nocase(['d','e','f']);

output(PARSE(ds,line,z,{MATCHTEXT},scan all));


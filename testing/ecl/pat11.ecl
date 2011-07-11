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


idRec := { unsigned4 id; };


r := record
  string line{maxlength(100)};
  dataset(idRec) ids{maxcount(10)};
end;

ds := dataset([
{'abcabcabcabcabcabcabcabcabcabcabcabcabcabcabc', [{1},{2},{3},{4}]},
{'abcABCabcABCabcabcABCabcABCabcabcABCabcABCabc', [{9},{8}]},
{'AabBcCdDeEfF', [{1}]},
{'', []}],r);

pattern a := 'abc';

pattern b := a nocase(a) a;

pattern S := b;

results := 
    record
        string x := MATCHTEXT;
        string y := MATCHTEXT(a);
        dataset(idRec) ids{maxcount(10)} := ds.ids;
    end;

//Another potential leak - not all results from a parse a read...
output(choosen(PARSE(ds,line,S,results,scan all),2));




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

export vstring(integer i9q) := TYPE
    export integer physicallength := i9q;
    export string load(string s) := s[1..i9q];
    export string store(string s) := s[1..i9q];
    END;

filledrecord(unsigned4 maxlen) :=
        record
           integer2 len;
           data9 cid;
           vstring(maxLen - SELF.len) filler;
         end;

input := filledrecord(99);

inputfile := dataset('xxx', input, flat);

person := dataset('person', { unsigned8 person_id, string1 per_sex, string2 per_st, string40 per_first_name, data9 per_cid }, thor);
otherinput := table(person, {per_cid, per_first_name});


outputrec := record
               integer2 len;
               data9 cid;
               string25 per_first_name;
               vstring(self.len) filler;
             end;

outputrec t(input L, otherinput R) := transform
       self.len := L.len + 25;
       self.cid := L.cid;
       self.per_first_name := R.per_first_name;
       self.filler := L.filler;
     END;

outputfile := join(inputfile, otherinput, LEFT.cid = right.per_cid,t(LEFT, RIGHT));

output(outputfile,, 'ccc')

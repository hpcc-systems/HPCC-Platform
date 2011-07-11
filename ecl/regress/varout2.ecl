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

pstring := type
    export integer physicallength(string x) := transfer(x[1],
unsigned1)+1;
    export string load(string x) := x[2..transfer(x[1], unsigned1)+1];
    export string store(string x) := transfer(length(x), string1)+x;
end;

input := record
           integer2 len;
           string5 cid;
         end;

inputfile := dataset('xxx', input, flat);

person := dataset('person', { unsigned8 person_id, string1 per_sex, string2 per_st, string40 per_first_name, data9 per_cid}, thor);
otherinput := table(person, {per_cid, per_first_name});

outputrec := record
               integer2 len;
               pstring cid;
             end;

outputrec t(input L, otherinput R) := transform
//     self.cid := L.cid;
       self := L;
     END;

outputfile := join(inputfile, otherinput, LEFT.cid = (string)right.per_cid,t(LEFT, RIGHT));

output(outputfile,, 'ccc')

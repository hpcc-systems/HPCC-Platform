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

#option ('pickBestEngine', false);

person := dataset('person', { unsigned8 person_id, string5 per_zip, string40 per_first_name, data9 per_cid, unsigned8 xpos }, thor);

myfac := map ( person.per_zip>'30000' and person.per_zip < '31000' => 1,
               person.per_zip = '44556' => 2,
               3);

s := sort(person,myfac);

ch := choosen(s,5000);

cnts := record
  p := person.per_zip;
  c := count(group);
  end;

gr := table(ch,cnts,per_zip);

//output(gr)


gr2 := table(person,{per_zip});

output(gr2);
output(person);
output(group(person));

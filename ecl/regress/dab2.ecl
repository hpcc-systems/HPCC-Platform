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

person := dataset('person', { unsigned8 person_id, string2 per_st, unsigned per_dob, string40 per_last_name }, thor);

HHSKey := person.per_st + person.per_last_name;

outrec :=
{
integer2 position := 0;
HHSKey,
xcnt := person.person_id;
};

t := table(choosen(person,1000),outrec);


sinf := sort(t,HHSKey,xcnt);

byh := group(sinf,HHSKey);

byhs := byh;//sort(byh,xcnt); //- RKC known bug

outrec itrans(outrec l, outrec r) := transform
  self.position := IF(l.position=0,2,l.position+1);
  self := r;
  end;

numbered := iterate(byhs,itrans(left,right));

threes := numbered(position=3);
twos := numbered(position=2);
others := numbered(position>3);

outrec make_two(outrec l, outrec r) := transform
  self.position := if (r.position = 0, 1, 2);
  self := l;
  end;

append_twos := join(twos,threes,left.HHSKey = right.HHSKey,make_two(left,right),left outer);

ofile := append_twos+threes+others;

output(ofile)
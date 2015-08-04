/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
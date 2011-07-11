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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string40 per_first_name, unsigned8 xpos }, thor);
inputlabel := person;

k1 := 100000;

scal(integer have, integer want) :=
(integer)(((want*(10000000/have))+99)/100);

scale := scal(count(inputlabel),55555) : stored('gavin');

mytf := record
  integer bucket := 0;
  boolean take := false;
  inputlabel;
  end;

mytf trans(mytf l, mytf r) := transform
  self.take := if(l.xpos=0,true,l.bucket+scale>k1);
  self.bucket := if(l.xpos=0,scale-k1,l.bucket+scale-if
(l.bucket+scale>k1,k1,0));
  self := r;
  end;

mytt := table(inputlabel,mytf);

tt := iterate(mytt,trans(left,right));

 count (tt)

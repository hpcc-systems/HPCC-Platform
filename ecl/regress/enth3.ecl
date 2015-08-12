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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string40 per_first_name, unsigned8 xpos }, thor);
inputlabel := person;

k1 := 100000;

scal(integer have, integer want) :=
(integer)(((want*(10000000/have))+99)/100);

scale := scal(count(inputlabel),55555);

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

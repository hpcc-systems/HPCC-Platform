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

/*
export samplemac1(inpset, outvar) := MACRO
    SHARED outvar := inpset(person.per_sex = 'M')
ENDMACRO;
export samplemac2(inpset, outvar) := MACRO
    SHARED outvar := inpset(person.per_sex = 'F')
ENDMACRO;
samplemac1(person, J1);
samplemac2(person, J2);
J := J1 + J2;
count(J);
*/
person := dataset('person', { unsigned8 person_id, string40 per_first_name }, thor);

p1 := person(per_first_name='C');
p2 := person(per_first_name='D');
p := p1 + p2;
count(p);


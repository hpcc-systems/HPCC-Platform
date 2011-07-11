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


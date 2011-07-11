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

maxperson := max(person(person_id < 1000), xpos);
global_maxperson := max(person(person_id < 1000), xpos) : global;


filtered := person(xpos < maxperson);
output(filtered,,'out.d00');

filtered2 := person(xpos < global_maxperson);
output(filtered2,,'out.d00');

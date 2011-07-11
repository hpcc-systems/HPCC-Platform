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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string10 per_first_name, string10 per_last_name, data9 per_cid }, thor);
a0 := person(per_last_name='XX');
output(a0,{ (string15) per_first_name, per_cid }, 'RKC::rkc');
a1 := dataset('RKC::rkc', { string15 per_first_name, data9 per_cid }, flat); 
/* both syntax should work */
a0 t1(a0 L, a1 R) := transform SELF:=L END;
typeof(a0) t2(a0 L, a1 R) := transform SELF:=L END;

// should at least report better error message
//t3(a0 L, a1 R) := transform SELF:=L END;

integer t4(a0 L, a1 R) := transform SELF:=L END;

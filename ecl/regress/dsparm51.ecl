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

// checkRecord

ds1 := dataset('ds',{string10 first_name; string10 last_name; }, flat);
ds2 := dataset('ds',{string10 first_name; }, flat);
ds3 := dataset('ds',{integer first_name; string10 last_name; }, flat);

add(virtual dataset d1, virtual dataset d2) := d1+d2;

// different field number
output(add(ds1(first_name = 'tom'), ds2(first_name='john')));

// type mismatch
output(add(ds1, ds3));

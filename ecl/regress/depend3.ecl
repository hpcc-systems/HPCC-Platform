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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string40 per_first_name, string40 per_last_name }, thor);
d := person(per_first_name='RICHARD');
output(d,{(string20) per_last_name},'rkc::x11');
output(d,{(string20) per_last_name},'rkc::x12');
d1 := dataset('rkc::x11', { string20 per_last_name }, THOR);
d2 := dataset('rkc::x12', { string20 per_last_name }, THOR);
output(choosen(d1(per_last_name='DRIMBAD')+d2(per_last_name='DRIMBAD'),1000000));


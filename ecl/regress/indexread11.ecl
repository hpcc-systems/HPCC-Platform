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

d := dataset('~local::rkc::person', { string15 f1, string15 f2, string15 f3, string15 f4, unsigned8 filepos{virtual(fileposition)} }, flat);

i := index(d, { d } ,'\\home\\person.name_first.key');


count(i(keyed(f1='Gavin1')));
count(i(keyed(f1='Gavin2' and f2='Hawthorn')));
count(i(keyed(f2='Gavin3') and wild(f1)));
count(i(f2='Hawthorn'));

count(i(keyed(f1='Gavin3') and keyed(f2='Gavin4') and wild(f1)));
count(i(f1='Gavin3' and keyed(f2='Gavin4') and wild(f1)));

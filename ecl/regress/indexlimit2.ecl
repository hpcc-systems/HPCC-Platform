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

#option ('globalFold', false);
d := dataset('~local::rkc::person', { string15 name, unsigned8 filepos{virtual(fileposition)} }, flat);

i := index(d, { f_name := (string11) name, filepos } ,'\\home\\person.name_first.key');

a1 := i(f_name='RICHARD');

a2 := limit(a1, 100, keyed, skip);

output(a2);

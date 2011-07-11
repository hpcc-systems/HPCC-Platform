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

#option ('sortIndexPayload', true);

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

i1 := index(namesTable, { age }, {surname, forename}, 'i1');                //NB: Default for whether this is handled as sorted is not dependent on sortIndexPayload setting
i2 := index(namesTable, { age }, {surname, forename}, 'i1', sort all);
i3 := index(namesTable, { age }, {surname, forename}, 'i1', sort keyed);

integer x := 0 : stored('x');

case(x,
1=>build(i1),
2=>build(i2),
3=>build(i3)
);

output(sort(sorted(i1), age));                              // should sort
output(sort(sorted(i2), age));                              // should sort
output(sort(sorted(i3), age));                              // should not sort
output(sort(sorted(i1), age, surname, forename));       // should not sort
output(sort(sorted(i2), age, surname, forename));       // should not sort
output(sort(sorted(i3), age, surname, forename));       // should sort

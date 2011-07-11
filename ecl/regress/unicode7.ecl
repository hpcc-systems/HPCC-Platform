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



ds := dataset('ds', {unicode40 name, unicode40 associate, unsigned6 did}, thor);


i := index(ds, { name }, { associate, did }, 'i');      // don't sort payload using memcmp

buildindex(i);

unicode40 searchName := U'' : stored('searchName');
unicode40 searchName2 := U'Ахалкалаки';

output(i(keyed(name = searchName)));

output(i(keyed(name[1..length(trim(searchName))] = trim(searchName))));

output(i(keyed(name[1..length(trim(searchName2))] = trim(searchName2))));


output(i(keyed(name[1..15] = trim(searchName2))));

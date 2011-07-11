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

person := dataset('person', { unsigned8 person_id }, thor);

// Following should be true

count(person(all >= all));
count(person(all>[1,2]));
count(person([1,2]<>all));

count(person(123 in ALL));
count(person('a' in ALL));

count(person([]!=all));
count(person(all!=[]));

// Following should be false

count(person(all > all));
count(person([1,2]>all));
count(person([1,2]=all));
count(person(145 not in ALL));

count(person(all=[]));
count(person([]=all));

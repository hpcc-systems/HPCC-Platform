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

person := dataset('person', { unsigned8 person_id, string1 per_sex }, thor);
rec := Record
  totalcount := SUM(GROUP, IF(TRUE, 1, 0));
  male := SUM(GROUP,IF(person.per_sex IN ['1'], 1, 0));
END;

tab := TABLE (person, rec); // This works against ***SOME*** clusters 
//tab := TABLE (person, rec, 1);    // This works against ***ALL*** clusters 
OUTPUT(tab);


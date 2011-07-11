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

person := dataset('person', { unsigned8 person_id, integer per_dbrth, unsigned8 xpos }, thor);
IfAge := IF (Person.per_dbrth <> -1, 1, 0);

r := RECORD
  NAge := SUM(GROUP, IfAge);
END;

counts := TABLE(Person, r);

// this is not legal
counts[1].NAge;

// should do this:
evaluate(counts[1],counts.NAge);

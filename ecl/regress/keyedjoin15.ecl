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

__set_debug_option__('targetClusterType', 'hthor');

mainRecord :=
        RECORD
integer8            sequence;
string20            forename;
string20            surname;
string20            alias;
unsigned8           filepos{virtual(fileposition)};
        END;

mainTable := dataset('~keyed.d00',mainRecord,THOR);

nameKey := INDEX(mainTable, { surname, forename, filepos }, 'name.idx');
sequenceKey := INDEX(mainTable, { sequence, filepos }, 'sequence.idx');


peopleDataset := dataset('people',mainRecord,THOR);

output(join(peopleDataset, nameKey, left.surname = right.surname and left.forename = right.forename, atmost(10)));
output(join(peopleDataset, nameKey, keyed(left.surname = right.surname and left.forename = right.forename), atmost(20)));
output(join(peopleDataset, nameKey, keyed(left.surname = right.surname) and left.forename = right.forename, atmost(30)));
output(join(peopleDataset, nameKey, left.surname = right.surname and left.forename = right.forename, atmost(left.surname=right.surname,40)));
output(join(peopleDataset, nameKey, keyed(left.surname = right.surname) and left.forename = right.forename, atmost(left.surname=right.surname, 50)));

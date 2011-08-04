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

#option ('optimizeIndexSource',true);

mainRecord :=
        RECORD
string20            forename;
string20            surname;
integer8            sequence;
unsigned8           filepos{virtual(fileposition)};
        END;

mainTable := dataset('~keyed.d00',mainRecord,THOR);

nameKey := INDEX(mainTable, { mainTable }, 'name.idx');

output(count(nameKey) > 99);

output(count(choosen(nameKey, 100)) > 99);

output(max(choosen(nameKey, 101), surname));
output(table(choosen(nameKey(forename != ''), 102), { count(group) }));
output(table(choosen(nameKey(forename != ''), 103), { max(group, forename) }));

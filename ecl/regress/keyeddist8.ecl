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

mainRecord := 
        RECORD
integer8            sequence;
string20            forename;
string20            alias;
unsigned6           surname;
unsigned8           filepos{virtual(fileposition)};
        END;

mainTable := dataset('~keyed.d00',mainRecord,THOR);

sequenceRecord := RECORD
        mainTable.sequence;
        mainTable.surname;
        mainTable.alias;
        mainTable.filepos;
    END;

nameKey := INDEX(mainTable, { surname, forename, filepos }, 'name.idx');

incTable := dataset('~inc.d00',mainRecord,THOR);

x := distribute(incTable, nameKey, (unsigned8)left.surname = (unsigned8)right.surname, first);

output(x);
